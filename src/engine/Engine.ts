import type { Clock } from '../clock/Clock.js';
import { SystemClock } from '../clock/Clock.js';
import type { Address, NodeIdentity, ReducerName } from '../domain/address.js';
import type { Context } from '../domain/context.js';
import type { ReducerPayload, WireMessage } from '../domain/message.js';
import { MessageType } from '../domain/message.js';
import type { TranslatedState } from '../domain/state.js';
import { PipelineError, PipelineStage, RsdpError } from '../errors.js';
import type { ExceptionFilter } from '../reducer/pipeline/stages.js';
import type { Reducer } from '../reducer/Reducer.js';
import type { Slan, Unsubscribe } from '../slan/Slan.js';
import { DebateBuffer } from './memory/DebateBuffer.js';
import { MemoryMap } from './memory/MemoryMap.js';
import { Fsm, Phase } from './phases/Fsm.js';
import type { DebounceConfig } from './schedule/Debouncer.js';
import { Debouncer } from './schedule/Debouncer.js';
import { RunQueue } from './schedule/RunQueue.js';

export interface EngineConfig {
    debounce: DebounceConfig;
    ttlMs: number;
    sweepIntervalMs: number;
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

interface ReducerSlot<Ctx extends Context> {
    reducer: Reducer<unknown, unknown, Ctx>;
    memory: MemoryMap<unknown>;
    debate: DebateBuffer<unknown>;
    queue: RunQueue;
    internal: unknown; // prev S
    translated: TranslatedState<unknown> | null;
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

class EngineImpl<Ctx extends Context> implements Engine<Ctx> {
    private readonly clock: Clock;
    private readonly fsm = new Fsm();
    private readonly slots = new Map<ReducerName, ReducerSlot<Ctx>>();
    private readonly convergedCbs = new Set<(s: StateSnapshot<Ctx>) => void>();
    private readonly errorCbs = new Set<(e: RsdpError) => void>();
    private steadyDebouncer!: Debouncer;
    private debateDebouncer!: Debouncer;
    private sweepTimer: ReturnType<Clock['setTimer']> | null = null;
    private unsub: Unsubscribe | null = null;
    private debatePending: Promise<void> = Promise.resolve();

    constructor(private readonly opts: CreateEngineOptions<Ctx>) {
        this.clock = opts.clock ?? new SystemClock();
        for (const r of opts.reducers) {
            const slot: ReducerSlot<Ctx> = {
                reducer: r as Reducer<unknown, unknown, Ctx>,
                memory: new MemoryMap<unknown>(),
                debate: new DebateBuffer<unknown>(),
                queue: new RunQueue(() => Promise.resolve()),
                internal: null,
                translated: null,
            };
            slot.queue = new RunQueue(() => this.runShare(slot));
            this.slots.set(r.name, slot);
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
        this.steadyDebouncer = new Debouncer(this.clock, this.opts.config.debounce, () => this.triggerAll());

        this.fsm.to(Phase.DEBATE);
        await this.opts.slan.broadcast({ type: MessageType.Hello, from: this.opts.identity.address });
        // Anchor the one-shot DEBATE window to the HELLO broadcast.
        this.debateDebouncer.notifyChange();

        this.sweepTimer = this.clock.setTimer(() => this.sweep(), this.opts.config.sweepIntervalMs);
    }

    async stop(): Promise<void> {
        await this.opts.slan.broadcast({
            type: MessageType.Close,
            from: this.opts.identity.address,
            closed: this.opts.identity.address,
        });
        if (this.sweepTimer !== null) this.clock.clearTimer(this.sweepTimer);
        this.debateDebouncer.cancel();
        this.steadyDebouncer.cancel();
        this.unsub?.();
        await this.opts.slan.close();
    }

    private async onMessage(msg: WireMessage, from: Address): Promise<void> {
        switch (msg.type) {
            case MessageType.Hello:
                // Reply with our current view as STATUS (full-consensus mode).
                await this.opts.slan.sendTo(from, {
                    type: MessageType.Status,
                    from: this.opts.identity.address,
                    payloads: this.composite(),
                });
                return;
            case MessageType.Status:
                this.ingest(
                    msg,
                    (slot, payload) => {
                        slot.debate.set(from, payload.value);
                        return true;
                    },
                    this.debateDebouncer,
                );
                return;
            case MessageType.Share:
                this.ingest(
                    msg,
                    (slot, payload) => slot.memory.update(from, payload.value, payload.version, this.clock.now()),
                    this.steadyDebouncer,
                );
                return;
            case MessageType.Close: {
                const closed = msg.closed ?? from;
                let changed = false;
                for (const slot of this.slots.values()) {
                    if (!slot.memory.evict(closed)) continue;
                    changed = true;
                    // In the full-consensus model a peer's wire payload is its *full*
                    // translated view, which transitively names the departed node. Those
                    // payloads are opaque (`unknown`) to the engine, so we cannot scrub the
                    // departed id from them surgically. Evicting only the departed peer's own
                    // Σ slot leaves it re-injected via every surviving peer's cached view, so
                    // it can never be forgotten. Invalidate all cached peer views on a real
                    // departure; survivors rebuild Σ from their next fresh SHAREs (driven by
                    // the debounced re-run + re-broadcast).
                    for (const [addr] of slot.memory.snapshot()) slot.memory.evict(addr);
                }
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
            const slot = this.slots.get(name);
            if (!slot) continue;
            // A peer that has not yet translated a value emits `value: null` in its
            // composite (e.g. a STATUS reply to HELLO before its DEBATE has run).
            // Such empty contributions must not be ingested, or `null` leaks into
            // reducer batches and pollutes the converged state forever.
            if (payload.value === null) continue;
            // Only treat the message as a real change when the slot actually updated
            // (stale/duplicate SHAREs return false and must NOT trigger a re-run).
            if (apply(slot, payload)) changed = true;
        }
        if (changed) debouncer.notifyChange();
    }

    private async runDebate(): Promise<void> {
        for (const slot of this.slots.values()) {
            try {
                const batch = slot.debate.snapshot();
                slot.internal = await slot.reducer.runStatus(batch, this.ctx(), slot.internal);
                slot.translated = await slot.reducer.translate(slot.internal, null, this.ctx());
                slot.debate.clear();
            } catch (err) {
                await this.emitError(slot.reducer, MessageType.Status, err);
            }
        }
        this.fsm.to(Phase.IDLE);
        await this.opts.slan.broadcast({
            type: MessageType.Share,
            from: this.opts.identity.address,
            payloads: this.composite(),
        });
        this.notifyConverged();
    }

    private triggerAll(): void {
        for (const slot of this.slots.values()) slot.queue.trigger();
    }

    private async runShare(slot: ReducerSlot<Ctx>): Promise<void> {
        try {
            const batch = slot.memory.snapshot().map(([, payload]) => payload);
            const prev = slot.internal;
            slot.internal = await slot.reducer.runShare(batch, this.ctx(), prev);
            const translated = await slot.reducer.translate(slot.internal, prev, this.ctx());
            slot.translated = translated;
            if (translated.changed) {
                await this.opts.slan.broadcast({
                    type: MessageType.Share,
                    from: this.opts.identity.address,
                    payloads: this.composite(),
                });
            }
            this.notifyConverged();
        } catch (err) {
            await this.emitError(slot.reducer, MessageType.Share, err);
        }
    }

    private sweep(): void {
        const now = this.clock.now();
        let changed = false;
        for (const slot of this.slots.values())
            changed = slot.memory.sweepExpired(now, this.opts.config.ttlMs).length > 0 || changed;
        if (changed) this.steadyDebouncer.notifyChange();
        this.sweepTimer = this.clock.setTimer(() => this.sweep(), this.opts.config.sweepIntervalMs);
    }

    private composite(): Record<ReducerName, ReducerPayload> {
        const out: Record<ReducerName, ReducerPayload> = {};
        for (const [name, slot] of this.slots) {
            out[name] = { value: slot.translated?.value ?? null, version: this.clock.now() };
        }
        return out;
    }

    private snapshotView(): StateSnapshot<Ctx> {
        const slots = this.slots;
        return {
            get<S, V>(reducer: Reducer<S, V, Ctx>): TranslatedState<V> | null {
                return (slots.get(reducer.name)?.translated as TranslatedState<V> | undefined) ?? null;
            },
        };
    }

    stateOf<S, V>(reducer: Reducer<S, V, Ctx>): TranslatedState<V> | null {
        return (this.slots.get(reducer.name)?.translated as TranslatedState<V> | undefined) ?? null;
    }
    stateByName(name: ReducerName): TranslatedState<unknown> | null {
        return this.slots.get(name)?.translated ?? null;
    }
    onConverged(cb: (s: StateSnapshot<Ctx>) => void): Unsubscribe {
        this.convergedCbs.add(cb);
        return () => this.convergedCbs.delete(cb);
    }
    onError(cb: (e: RsdpError) => void): Unsubscribe {
        this.errorCbs.add(cb);
        return () => this.errorCbs.delete(cb);
    }
    async settle(): Promise<void> {
        await this.debatePending;
        await Promise.all([...this.slots.values()].map((s) => s.queue.idle()));
    }

    private notifyConverged(): void {
        const snap = this.snapshotView();
        this.opts.observer?.(snap);
        for (const cb of this.convergedCbs) cb(snap);
    }

    private filtersFor(reducer: Reducer<unknown, unknown, Ctx>, stage: MessageType): ExceptionFilter<Ctx>[] {
        switch (stage) {
            case MessageType.Status:
                return reducer.exceptionFilters.status;
            case MessageType.Share:
                return reducer.exceptionFilters.share;
            case MessageType.Close:
                return reducer.exceptionFilters.close;
            default:
                return [];
        }
    }

    private async emitError(reducer: Reducer<unknown, unknown, Ctx>, stage: MessageType, err: unknown): Promise<void> {
        const wrapped =
            err instanceof RsdpError
                ? err
                : new PipelineError(err instanceof Error ? err.message : String(err), {
                      reducer: reducer.name,
                      stage: PipelineStage.Aggregator,
                      messageType: stage,
                  });

        // Give the reducer's pipeline exception filters a chance to handle the error
        // before escalating to onError. A filter that throws falls through to onError.
        for (const filter of this.filtersFor(reducer, stage)) {
            try {
                await filter.handle(wrapped, this.ctx());
            } catch {
                // Filter rejected/failed — fall back to the global onError escalation.
            }
        }

        for (const cb of this.errorCbs) cb(wrapped);
    }
}

export function createEngine<Ctx extends Context>(opts: CreateEngineOptions<Ctx>): Engine<Ctx> {
    return new EngineImpl<Ctx>(opts);
}
