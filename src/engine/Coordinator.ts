import type { Clock } from '../clock/Clock.js';
import type { Address } from '../domain/address.js';
import type { Context } from '../domain/context.js';
import { MessageType } from '../domain/message.js';
import type { Unsubscribe } from '../slan/Slan.js';
import type { ErrorChannel } from './channels/ErrorChannel.js';
import type { OutboundChannel } from './channels/OutboundChannel.js';
import type { StateSnapshot } from './Engine.js';
import type { DebounceConfig } from './schedule/Debouncer.js';
import { Debouncer } from './schedule/Debouncer.js';
import { Fsm, Phase } from './schedule/Fsm.js';
import { RunQueue } from './schedule/RunQueue.js';
import type { SlotRegistry } from './state/SlotRegistry.js';

export interface CoordinatorOptions<Ctx extends Context> {
    clock: Clock;
    registry: SlotRegistry<Ctx>;
    outbound: OutboundChannel;
    errors: ErrorChannel<Ctx>;
    self: Address;
    debounce: DebounceConfig;
    resyncIntervalMs: number;
    observer?: ((snapshot: StateSnapshot<Ctx>) => void) | undefined;
}

/**
 * Drives the convergence lifecycle: the bootstrap DEBATE, steady-state recompute, and
 * the periodic resync. Owns the debouncers, the single steady run queue, the FSM and
 * the resync timer; emits one composite SHARE per cycle and notifies converged
 * observers. The engine pokes it via `scheduleDebate()`/`scheduleSteady()` — inbound
 * message parsing lives in {@link InboundRouter}, not here.
 */
export class Coordinator<Ctx extends Context> {
    private readonly clock: Clock;
    private readonly registry: SlotRegistry<Ctx>;
    private readonly outbound: OutboundChannel;
    private readonly errors: ErrorChannel<Ctx>;
    private readonly self: Address;
    private readonly resyncIntervalMs: number;
    private readonly observer?: ((snapshot: StateSnapshot<Ctx>) => void) | undefined;

    private readonly fsm = new Fsm();
    private readonly debateDebouncer: Debouncer;
    private readonly steadyDebouncer: Debouncer;
    private readonly steadyQueue = new RunQueue(() => this.runSteady());
    private readonly convergedCbs = new Set<(s: StateSnapshot<Ctx>) => void>();

    private resyncTimer: ReturnType<Clock['setTimer']> | null = null;
    private debatePending: Promise<void> = Promise.resolve();

    constructor(opts: CoordinatorOptions<Ctx>) {
        this.clock = opts.clock;
        this.registry = opts.registry;
        this.outbound = opts.outbound;
        this.errors = opts.errors;
        this.self = opts.self;
        this.resyncIntervalMs = opts.resyncIntervalMs;
        this.observer = opts.observer;

        this.debateDebouncer = new Debouncer(this.clock, opts.debounce, () => {
            this.debatePending = this.runDebate();
        });
        this.steadyDebouncer = new Debouncer(this.clock, opts.debounce, () => this.steadyQueue.trigger());
    }

    /** Bootstrap: announce via HELLO and anchor the first DEBATE window to it. */
    async start(): Promise<void> {
        this.fsm.to(Phase.DEBATE);
        await this.outbound.hello();
        this.debateDebouncer.notifyChange();
    }

    stop(): void {
        if (this.resyncTimer !== null) this.clock.clearTimer(this.resyncTimer);
        this.debateDebouncer.cancel();
        this.steadyDebouncer.cancel();
    }

    /** Poke a DEBATE round (incoming STATUS gathered). */
    scheduleDebate(): void {
        this.debateDebouncer.notifyChange();
    }
    /** Poke a steady-state recompute (Σ changed via SHARE / CLOSE / TTL). */
    scheduleSteady(): void {
        this.steadyDebouncer.notifyChange();
    }

    onConverged(cb: (s: StateSnapshot<Ctx>) => void): Unsubscribe {
        this.convergedCbs.add(cb);
        return () => this.convergedCbs.delete(cb);
    }

    /** Resolve once the in-flight DEBATE and all steady-state runs are idle. */
    async settle(): Promise<void> {
        await this.debatePending;
        await this.steadyQueue.idle();
    }

    private ctx(): Ctx {
        return { self: this.self } as Ctx;
    }

    private async runDebate(): Promise<void> {
        for (const slot of this.registry.values()) {
            try {
                await slot.runDebate(this.ctx());
            } catch (err) {
                await this.errors.emitPipeline(slot.reducer, MessageType.Status, err, this.ctx());
            }
        }
        this.fsm.to(Phase.IDLE);
        await this.outbound.share(this.registry.composite());
        // Enter steady state: start the periodic resync (re-HELLO) so a converged
        // cluster does not go silent — each round re-gathers peers' STATUS, re-derives
        // the view, and re-broadcasts SHARE, keeping liveness fresh (TTL only evicts
        // genuinely-departed nodes) and folding in late joiners / missed updates.
        this.resyncTimer ??= this.clock.setTimer(() => this.resync(), this.resyncIntervalMs);
        this.notifyConverged();
    }

    /**
     * Periodic resync: re-broadcast HELLO and re-anchor the bounded DEBATE window,
     * redoing the discovery handshake so peers reply with STATUS and we re-derive our
     * view. Re-gathering perspectives (rather than pushing our possibly-stale aggregate)
     * is what makes the protocol eventually consistent; the resulting SHARE refreshes
     * our liveness in peers' `Σ` so TTL only evicts genuinely-departed nodes.
     */
    private resync(): void {
        // Re-enter DEBATE to re-gather perspectives. If a DEBATE round is already in
        // flight (state still converging), it already serves the resync — just re-arm.
        if (this.fsm.phase === Phase.IDLE) {
            this.fsm.to(Phase.DEBATE);
            void this.outbound.hello();
            this.debateDebouncer.notifyChange();
        }
        this.resyncTimer = this.clock.setTimer(() => this.resync(), this.resyncIntervalMs);
    }

    /**
     * One steady-state convergence cycle: recompute every reducer over its current Σ,
     * then emit a SINGLE composite SHARE if any reducer's view changed (mirroring
     * runDebate). Run-queued so overlapping triggers coalesce into one trailing cycle.
     */
    private async runSteady(): Promise<void> {
        let changed = false;
        for (const slot of this.registry.values()) {
            try {
                if (await slot.runShare(this.ctx())) changed = true;
            } catch (err) {
                await this.errors.emitPipeline(slot.reducer, MessageType.Share, err, this.ctx());
            }
        }
        if (changed) await this.outbound.share(this.registry.composite());
        this.notifyConverged();
    }

    private notifyConverged(): void {
        const snap = this.registry.snapshotView();
        this.observer?.(snap);
        for (const cb of this.convergedCbs) cb(snap);
    }
}
