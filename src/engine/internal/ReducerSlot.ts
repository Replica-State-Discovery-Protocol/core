import type { Address, ReducerName } from '../../domain/address.js';
import type { Context } from '../../domain/context.js';
import type { ReducerPayload } from '../../domain/message.js';
import type { TranslatedState } from '../../domain/state.js';
import type { Reducer } from '../../reducer/Reducer.js';
import { DebateBuffer } from '../memory/DebateBuffer.js';
import { MemoryMap } from '../memory/MemoryMap.js';
import { RunQueue } from '../schedule/RunQueue.js';

/**
 * Owns one reducer's convergence state: the per-peer SHARE memory `Σ`, the transient
 * DEBATE buffer, a single-flight run queue, the last derived view, and a monotonic
 * outbound version (bumped only when the derived value actually changes — decoupling
 * wire versioning from the wall clock). The cross-slot SHARE broadcast stays with the
 * orchestrator; this class only derives its own slice of the composite state.
 */
export class ReducerSlot<Ctx extends Context> {
    private readonly memory = new MemoryMap<unknown>();
    private readonly debate = new DebateBuffer<unknown>();
    private queue = new RunQueue(() => Promise.resolve());
    private internal: unknown = null; // prev S
    private translated: TranslatedState<unknown> | null = null;
    private outVersion = 0;

    constructor(readonly reducer: Reducer<unknown, unknown, Ctx>) {}

    get name(): ReducerName {
        return this.reducer.name;
    }
    get state(): TranslatedState<unknown> | null {
        return this.translated;
    }
    /** This slot's entry in a composite wire message. */
    toPayload(): ReducerPayload {
        return { value: this.translated?.value ?? null, version: this.outVersion };
    }

    /** Bind the single-flight task this slot's steady-state queue runs. */
    attachShareRunner(run: () => Promise<void>): void {
        this.queue = new RunQueue(run);
    }
    trigger(): void {
        this.queue.trigger();
    }
    idle(): Promise<void> {
        return this.queue.idle();
    }

    /** STATUS contribution → transient DEBATE buffer. Always a candidate change. */
    ingestStatus(from: Address, value: unknown): boolean {
        this.debate.set(from, value);
        return true;
    }
    /** SHARE contribution → version-gated `Σ` slot. Returns whether the value changed. */
    ingestShare(from: Address, value: unknown, version: number, now: number): boolean {
        return this.memory.update(from, value, version, now);
    }

    /**
     * Evict a departed peer. In the full-consensus model a peer's payload is its *full*
     * translated view, which transitively names the departed node; those payloads are
     * opaque here, so evicting only the departed peer's own `Σ` slot leaves it re-injected
     * via every surviving peer's cached view. Invalidate all cached views on a real
     * departure; survivors rebuild `Σ` from their next fresh SHAREs. Returns whether
     * anything was evicted.
     */
    evictDeparted(closed: Address): boolean {
        if (!this.memory.evict(closed)) return false;
        for (const [addr] of this.memory.snapshot()) this.memory.evict(addr);
        return true;
    }
    /** TTL sweep. Returns whether any peer was evicted. */
    sweep(now: number, ttlMs: number): boolean {
        return this.memory.sweepExpired(now, ttlMs).length > 0;
    }

    /**
     * DEBATE round: converge over the union of this round's STATUS buffer and the
     * retained `Σ` perspectives, so a peer briefly silent for one resync round is held
     * by `Σ` until its TTL genuinely lapses. Compares against the prior state so an
     * unchanged round does not bump the version.
     */
    async runDebate(ctx: Ctx): Promise<void> {
        const batch = [...this.debate.snapshot(), ...this.memory.snapshot().map(([, payload]) => payload)];
        const prev = this.internal;
        this.internal = await this.reducer.runStatus(batch, ctx, prev);
        this.translated = await this.reducer.translate(this.internal, prev, ctx);
        if (this.translated.changed) this.outVersion += 1;
        this.debate.clear();
    }

    /** Steady-state aggregation over `Σ`. Returns whether the derived view changed. */
    async runShare(ctx: Ctx): Promise<boolean> {
        const batch = this.memory.snapshot().map(([, payload]) => payload);
        const prev = this.internal;
        this.internal = await this.reducer.runShare(batch, ctx, prev);
        this.translated = await this.reducer.translate(this.internal, prev, ctx);
        if (this.translated.changed) {
            this.outVersion += 1;
            return true;
        }
        return false;
    }
}
