import type { Clock } from '../clock/Clock.js';
import { SystemClock } from '../clock/Clock.js';
import type { Address, NodeIdentity, ReducerName } from '../domain/address.js';
import type { Context } from '../domain/context.js';
import type { ReducerPayload, WireMessage } from '../domain/message.js';
import { MessageType } from '../domain/message.js';
import type { TranslatedState } from '../domain/state.js';
import type { RsdpError } from '../errors.js';
import type { Reducer } from '../reducer/Reducer.js';
import type { Slan, Unsubscribe } from '../slan/Slan.js';
import { ErrorChannel } from './internal/ErrorChannel.js';
import { OutboundChannel } from './internal/OutboundChannel.js';
import { ReducerSlot } from './internal/ReducerSlot.js';
import { SlotRegistry } from './internal/SlotRegistry.js';
import { Fsm, Phase } from './phases/Fsm.js';
import type { DebounceConfig } from './schedule/Debouncer.js';
import { Debouncer } from './schedule/Debouncer.js';

export interface EngineConfig {
    debounce: DebounceConfig;
    ttlMs: number;
    sweepIntervalMs: number;
    /**
     * Periodic resync period: every `resyncIntervalMs` a node re-broadcasts HELLO,
     * redoing the discovery handshake — peers reply with STATUS, the node re-derives
     * its view (fresh DEBATE round) and re-broadcasts SHARE. This re-gathers
     * perspectives (folding in late joiners / missed updates / healed partitions) and
     * refreshes liveness. MUST be `< ttlMs`, or TTL would evict healthy members
     * between resyncs.
     */
    resyncIntervalMs: number;
}

export interface StateSnapshot<Ctx extends Context> {
    get<S, V>(reducer: Reducer<S, V, Ctx>): TranslatedState<V> | null;
}

export interface Engine<Ctx extends Context> {
    start(): Promise<void>;
    stop(): Promise<void>;
    stateOf<S, V>(reducer: Reducer<S, V, Ctx>): TranslatedState<V> | null;
    stateByName(name: ReducerName): TranslatedState<unknown> | null;
    onConverged(cb: (snapshot: StateSnapshot<Ctx>) => void): Unsubscribe;
    onError(cb: (err: RsdpError) => void): Unsubscribe;
    /** Test/operational aid: resolve once all pending reducer runs are idle. */
    settle(): Promise<void>;
}

export interface CreateEngineOptions<Ctx extends Context> {
    identity: NodeIdentity;
    slan: Slan;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    reducers: Reducer<any, any, Ctx>[];
    config: EngineConfig;
    clock?: Clock;
    observer?: (snapshot: StateSnapshot<Ctx>) => void;
}

/**
 * Orchestrator. Owns the protocol lifecycle (FSM, timers, message routing) and wires
 * together the collaborators that do the work: per-reducer convergence ({@link ReducerSlot}
 * via {@link SlotRegistry}), outbound messaging ({@link OutboundChannel}) and error
 * routing ({@link ErrorChannel}). The convergence math, wire composition and transport
 * boilerplate live in those modules, not here.
 */
class EngineImpl<Ctx extends Context> implements Engine<Ctx> {
    private readonly clock: Clock;
    private readonly fsm = new Fsm();
    private readonly registry = new SlotRegistry<Ctx>();
    private readonly errors = new ErrorChannel<Ctx>();
    private readonly outbound: OutboundChannel;
    private readonly convergedCbs = new Set<(s: StateSnapshot<Ctx>) => void>();
    private steadyDebouncer!: Debouncer;
    private debateDebouncer!: Debouncer;
    private sweepTimer: ReturnType<Clock['setTimer']> | null = null;
    private resyncTimer: ReturnType<Clock['setTimer']> | null = null;
    private unsub: Unsubscribe | null = null;
    private debatePending: Promise<void> = Promise.resolve();

    constructor(private readonly opts: CreateEngineOptions<Ctx>) {
        this.clock = opts.clock ?? new SystemClock();
        this.outbound = new OutboundChannel(opts.slan, opts.identity.address, this.errors);
        for (const r of opts.reducers) {
            const slot = new ReducerSlot<Ctx>(r as Reducer<unknown, unknown, Ctx>);
            slot.attachShareRunner(() => this.onSlotShare(slot));
            this.registry.add(slot);
        }
    }

    private ctx(): Ctx {
        return { self: this.opts.identity.address } as Ctx;
    }

    async start(): Promise<void> {
        this.unsub = this.opts.slan.onMessage((m, from) => void this.onMessage(m, from));
        await this.opts.slan.init();

        this.debateDebouncer = new Debouncer(this.clock, this.opts.config.debounce, () => {
            this.debatePending = this.runDebate();
        });
        this.steadyDebouncer = new Debouncer(this.clock, this.opts.config.debounce, () => this.registry.triggerAll());

        this.fsm.to(Phase.DEBATE);
        await this.outbound.hello();
        // Anchor the DEBATE window to the HELLO broadcast.
        this.debateDebouncer.notifyChange();

        this.sweepTimer = this.clock.setTimer(() => this.sweep(), this.opts.config.sweepIntervalMs);
    }

    async stop(): Promise<void> {
        await this.outbound.close();
        if (this.sweepTimer !== null) this.clock.clearTimer(this.sweepTimer);
        if (this.resyncTimer !== null) this.clock.clearTimer(this.resyncTimer);
        this.debateDebouncer.cancel();
        this.steadyDebouncer.cancel();
        this.unsub?.();
        await this.opts.slan.close();
    }

    private async onMessage(msg: WireMessage, from: Address): Promise<void> {
        switch (msg.type) {
            case MessageType.Hello:
                // Reply with our current view as STATUS (full-consensus mode).
                await this.outbound.status(from, this.registry.composite());
                return;
            case MessageType.Status:
                this.ingest(msg, (slot, p) => slot.ingestStatus(from, p.value), this.debateDebouncer);
                return;
            case MessageType.Share:
                this.ingest(
                    msg,
                    (slot, p) => slot.ingestShare(from, p.value, p.version, this.clock.now()),
                    this.steadyDebouncer,
                );
                return;
            case MessageType.Close: {
                const closed = msg.closed ?? from;
                let changed = false;
                for (const slot of this.registry.values()) if (slot.evictDeparted(closed)) changed = true;
                if (changed) this.steadyDebouncer.notifyChange();
                return;
            }
        }
    }

    private ingest(
        msg: WireMessage,
        apply: (slot: ReducerSlot<Ctx>, payload: ReducerPayload) => boolean,
        debouncer: Debouncer,
    ): void {
        if (!msg.payloads) return;
        let changed = false;
        for (const [name, payload] of Object.entries(msg.payloads)) {
            const slot = this.registry.get(name);
            if (!slot) continue;
            // A peer that has not yet translated a value emits `value: null` in its
            // composite (e.g. a STATUS reply to HELLO before its DEBATE has run). Such
            // empty contributions must not be ingested, or `null` leaks into reducer
            // batches and pollutes the converged state forever.
            if (payload.value === null) continue;
            // Only treat the message as a real change when the slot actually updated
            // (stale/duplicate SHAREs return false and must NOT trigger a re-run).
            if (apply(slot, payload)) changed = true;
        }
        if (changed) debouncer.notifyChange();
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
        this.resyncTimer ??= this.clock.setTimer(() => this.resync(), this.opts.config.resyncIntervalMs);
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
            // Anchor a fresh DEBATE window to this resync HELLO, mirroring start(); this
            // also guarantees runDebate fires (and returns us to IDLE) even with no peers
            // replying, so the FSM never gets stuck in DEBATE.
            this.debateDebouncer.notifyChange();
        }
        this.resyncTimer = this.clock.setTimer(() => this.resync(), this.opts.config.resyncIntervalMs);
    }

    /** Single-flight steady-state run for one slot; broadcasts the composite on change. */
    private async onSlotShare(slot: ReducerSlot<Ctx>): Promise<void> {
        try {
            if (await slot.runShare(this.ctx())) await this.outbound.share(this.registry.composite());
            this.notifyConverged();
        } catch (err) {
            await this.errors.emitPipeline(slot.reducer, MessageType.Share, err, this.ctx());
        }
    }

    private sweep(): void {
        const now = this.clock.now();
        let changed = false;
        for (const slot of this.registry.values()) changed = slot.sweep(now, this.opts.config.ttlMs) || changed;
        if (changed) this.steadyDebouncer.notifyChange();
        this.sweepTimer = this.clock.setTimer(() => this.sweep(), this.opts.config.sweepIntervalMs);
    }

    stateOf<S, V>(reducer: Reducer<S, V, Ctx>): TranslatedState<V> | null {
        return this.registry.stateOf(reducer);
    }
    stateByName(name: ReducerName): TranslatedState<unknown> | null {
        return this.registry.stateByName(name);
    }
    onConverged(cb: (s: StateSnapshot<Ctx>) => void): Unsubscribe {
        this.convergedCbs.add(cb);
        return () => this.convergedCbs.delete(cb);
    }
    onError(cb: (e: RsdpError) => void): Unsubscribe {
        return this.errors.subscribe(cb);
    }
    async settle(): Promise<void> {
        await this.debatePending;
        await this.registry.idleAll();
    }

    private notifyConverged(): void {
        const snap = this.registry.snapshotView();
        this.opts.observer?.(snap);
        for (const cb of this.convergedCbs) cb(snap);
    }
}

export function createEngine<Ctx extends Context>(opts: CreateEngineOptions<Ctx>): Engine<Ctx> {
    return new EngineImpl<Ctx>(opts);
}
