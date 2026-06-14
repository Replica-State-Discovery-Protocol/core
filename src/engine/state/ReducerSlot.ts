import type { Address, ReducerName } from '../../domain/address.js';
import type { Context } from '../../domain/context.js';
import type { ReducerPayload } from '../../domain/message.js';
import type { TranslatedState } from '../../domain/state.js';
import type { Reducer } from '../../reducer/Reducer.js';
import { DebateBuffer } from './DebateBuffer.js';
import { MemoryMap } from './MemoryMap.js';

/**
 * Owns one reducer's convergence state: the per-peer SHARE memory `Σ`, the transient
 * DEBATE buffer, the last derived view, and a monotonic outbound version (bumped only
 * when the derived value actually changes — decoupling wire versioning from the wall
 * clock). Pure state-derivation: it produces only its own slice of the composite. All
 * lifecycle/messaging (run scheduling, the single composite SHARE broadcast) lives in
 * the engine; a slot never emits a message of its own.
 */
export class ReducerSlot<Ctx extends Context> {
    private readonly memory = new MemoryMap<unknown>();
    private readonly debate = new DebateBuffer<unknown>();
    private readonly departed = new Set<Address>();

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
     * CLOSE contribution → transient departed-address buffer (consumed by the CLOSE phase).
     * Buffers only peers actually present in `Σ`, mirroring "ignore the close of a peer we
     * never knew"; returns whether a new departure was buffered (the trigger for a CLOSE
     * round).
     */
    ingestClose(addr: Address): boolean {
        if (!this.memory.has(addr) || this.departed.has(addr)) return false;
        this.departed.add(addr);
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

    /**
     * CLOSE round: run the reducer's close pipeline over the buffered departed addresses
     * (which derive a new state with those peers removed), then drop their `Σ` slots so a
     * later steady run cannot re-aggregate them. Survivors that still name a departed peer
     * in their cached view correct it via their own CLOSE round + re-SHARE. Returns whether
     * the derived view changed.
     */
    async runClose(ctx: Ctx): Promise<boolean> {
        if (this.departed.size === 0) return false;
        const batch = [...this.departed];
        const prev = this.internal;

        this.internal = await this.reducer.runClose(batch, ctx, prev);
        this.translated = await this.reducer.translate(this.internal, prev, ctx);

        for (const addr of batch) this.memory.evict(addr);
        this.departed.clear();

        if (this.translated.changed) {
            this.outVersion += 1;
            return true;
        }

        return false;
    }
}
