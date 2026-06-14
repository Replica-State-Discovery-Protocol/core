import type { Clock } from '../clock/Clock.js';
import type { Address } from '../domain/address.js';
import type { Context } from '../domain/context.js';
import { MessageType } from '../domain/message.js';
import type { Unsubscribe } from '../slan/Slan.js';
import type { ErrorChannel } from './channels/ErrorChannel.js';
import type { OutboundChannel } from './channels/OutboundChannel.js';
import type { StateSnapshot } from './Engine.js';
import type { DebounceConfig } from './schedule/Debouncer.js';
import { Phase } from './schedule/Fsm.js';
import { PhaseScheduler } from './schedule/PhaseScheduler.js';
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
 * Drives the convergence lifecycle: the bootstrap DEBATE, steady-state recompute, the
 * departure CLOSE, and the periodic resync. It owns the {@link PhaseScheduler} (which
 * gates and serializes all phase work) and the resync timer; each phase handler emits at
 * most one composite SHARE and notifies converged observers. The engine pokes it via
 * `scheduleDebate()`/`scheduleSteady()`/`scheduleClose()` — inbound message parsing lives
 * in {@link InboundRouter}, not here. The handlers never touch the FSM directly; the
 * scheduler transitions into a phase before invoking the handler and back to IDLE after.
 */
export class Coordinator<Ctx extends Context> {
    private readonly clock: Clock;
    private readonly registry: SlotRegistry<Ctx>;
    private readonly outbound: OutboundChannel;
    private readonly errors: ErrorChannel<Ctx>;
    private readonly self: Address;
    private readonly resyncIntervalMs: number;
    private readonly observer?: ((snapshot: StateSnapshot<Ctx>) => void) | undefined;

    private readonly scheduler: PhaseScheduler;
    private readonly convergedCbs = new Set<(s: StateSnapshot<Ctx>) => void>();

    private resyncTimer: ReturnType<Clock['setTimer']> | null = null;

    constructor(opts: CoordinatorOptions<Ctx>) {
        this.clock = opts.clock;
        this.registry = opts.registry;
        this.outbound = opts.outbound;
        this.errors = opts.errors;
        this.self = opts.self;
        this.resyncIntervalMs = opts.resyncIntervalMs;
        this.observer = opts.observer;

        this.scheduler = new PhaseScheduler({
            clock: this.clock,
            debounce: opts.debounce,
            handlers: {
                [Phase.DEBATE]: () => this.runDebate(),
                [Phase.SHARE]: () => this.runSteady(),
                [Phase.CLOSE]: () => this.runClose(),
            },
        });
    }

    /**
     * Bootstrap: announce via HELLO and request the first DEBATE round (the scheduler
     * debounces it into a window so STATUS replies are gathered first). Arm the periodic
     * resync so a converged cluster does not go silent.
     */
    async start(): Promise<void> {
        await this.outbound.hello();

        this.scheduler.request(Phase.DEBATE);

        this.resyncTimer = this.clock.setTimer(() => void this.resync(), this.resyncIntervalMs);
    }

    stop(): void {
        if (this.resyncTimer !== null) this.clock.clearTimer(this.resyncTimer);

        this.scheduler.stop();
    }

    /** Poke a DEBATE round (incoming STATUS gathered). */
    scheduleDebate(): void {
        this.scheduler.request(Phase.DEBATE);
    }
    /** Poke a steady-state recompute (Σ changed via SHARE / TTL sweep). */
    scheduleSteady(): void {
        this.scheduler.request(Phase.SHARE);
    }
    /** Poke a departure CLOSE round (a peer's CLOSE was buffered). */
    scheduleClose(): void {
        this.scheduler.request(Phase.CLOSE);
    }

    onConverged(cb: (s: StateSnapshot<Ctx>) => void): Unsubscribe {
        this.convergedCbs.add(cb);
        return () => this.convergedCbs.delete(cb);
    }

    /** Resolve once no phase is executing (test/operational aid). */
    settle(): Promise<void> {
        return this.scheduler.settle();
    }

    private ctx(): Ctx {
        return { self: this.self } as Ctx;
    }

    /**
     * DEBATE round: re-derive every reducer over the union of its STATUS buffer and Σ,
     * then always emit one composite SHARE — even when unchanged, so the broadcast
     * refreshes our liveness in peers' Σ (bootstrap and every resync run through here).
     */
    private async runDebate(): Promise<void> {
        for (const slot of this.registry.values()) {
            try {
                await slot.runDebate(this.ctx());
            } catch (err) {
                await this.errors.emitPipeline(slot.reducer, MessageType.Status, err, this.ctx());
            }
        }
        await this.outbound.share(this.registry.composite());
        this.notifyConverged();
    }

    /**
     * Periodic resync: re-broadcast HELLO and request a DEBATE round, redoing the
     * discovery handshake so peers reply with STATUS and we re-derive our view.
     * Re-gathering perspectives (rather than pushing our possibly-stale aggregate) is what
     * makes the protocol eventually consistent; the resulting SHARE refreshes our liveness
     * in peers' Σ. If a DEBATE is already scheduled (window open, queued, or running) it
     * already serves the resync — skip rather than stack a second HELLO. The timer always
     * re-arms, so a round deferred behind other work is retried, never silently dropped.
     *
     * The returned promise is awaited by nothing (the timer discards it); it is `async`
     * only so the HELLO broadcast is genuinely awaited. The periodic timer is re-armed
     * synchronously, up front, so its cadence never depends on the broadcast resolving.
     */
    private async resync(): Promise<void> {
        this.resyncTimer = this.clock.setTimer(() => void this.resync(), this.resyncIntervalMs);

        if (!this.scheduler.isScheduled(Phase.DEBATE)) {
            await this.outbound.hello();
            this.scheduler.request(Phase.DEBATE);
        }
    }

    /**
     * Steady-state round: recompute every reducer over its current Σ, then emit a single
     * composite SHARE if any reducer's view changed.
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

    /**
     * CLOSE round: run every reducer's close pipeline over its buffered departures
     * (removing them from the derived view and evicting their Σ slots), then emit a single
     * composite SHARE if any view changed.
     */
    private async runClose(): Promise<void> {
        let changed = false;
        for (const slot of this.registry.values()) {
            try {
                if (await slot.runClose(this.ctx())) changed = true;
            } catch (err) {
                await this.errors.emitPipeline(slot.reducer, MessageType.Close, err, this.ctx());
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
