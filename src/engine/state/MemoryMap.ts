import type { Address } from '../../domain/address.js';
import type { PeerRecord } from '../../domain/peer.js';
import type { Incarnation, IncarnationValue } from '../../incarnation/Incarnation.js';

export class MemoryMap<Payload> {
    private readonly records = new Map<Address, PeerRecord<Payload>>();

    constructor(private readonly incarnation: Incarnation) {}

    /**
     * Lexicographic `(ι, v)` gate (eq. 15), falling back to the version-only gates of
     * eq. (3) within a single lifetime. Returns true iff the stored VALUE changed.
     *
     * - `ι > stored` → a restarted peer: adopt **even if its version is lower**, since a
     *   restart resets the counter (§2.2) and the two numbers describe different lifetimes.
     * - `ι < stored` → a message from a dead lifetime: ignore entirely, and in particular
     *   do not refresh liveness, or a delayed straggler could postpone a real eviction.
     * - same `ι` (or `ι` absent on either side — a peer that does not speak incarnations):
     *   - `version < existing.version` → stale, ignored entirely (no liveness refresh).
     *   - `version === existing.version` → a periodic re-broadcast (resync): refresh
     *     `lastSeenAt` so TTL keeps the live peer, but keep payload/version and report
     *     no state change (`false`). Liveness is refreshed without a recompute storm.
     *   - `version > existing.version` (or no existing) → store the new payload, refresh
     *     liveness, report a state change (`true`).
     */
    update(addr: Address, payload: Payload, version: number, now: number, inc?: IncarnationValue): boolean {
        const existing = this.records.get(addr);
        if (existing) {
            const lifetime = this.order(inc, existing.inc);
            if (lifetime < 0) return false;
            if (lifetime === 0) {
                if (version < existing.version) return false;
                if (version === existing.version) {
                    existing.lastSeenAt = now;
                    return false;
                }
            }
        }
        this.records.set(addr, { payload, version, inc, lastSeenAt: now });
        return true;
    }

    /**
     * Whether `inc` names a strictly older lifetime than the one on record — the read-only
     * check used to drop ungated traffic (a STATUS or CLOSE redelivered from a queue long
     * after the sender restarted). False for an unknown peer, and false whenever either
     * side carries no `ι`, so an unknown lifetime never suppresses a message.
     */
    isOlderIncarnation(addr: Address, inc: IncarnationValue | undefined): boolean {
        const existing = this.records.get(addr);
        return existing !== undefined && this.order(inc, existing.inc) < 0;
    }

    evict(addr: Address): boolean {
        return this.records.delete(addr);
    }

    has(addr: Address): boolean {
        return this.records.has(addr);
    }

    /** Remove peers unseen for >= ttlMs. Returns the evicted addresses. */
    sweepExpired(now: number, ttlMs: number): Address[] {
        const evicted: Address[] = [];
        for (const [addr, rec] of this.records) {
            if (now - rec.lastSeenAt >= ttlMs) {
                this.records.delete(addr);
                evicted.push(addr);
            }
        }
        return evicted;
    }

    snapshot(): readonly (readonly [Address, Payload])[] {
        return [...this.records.entries()].map(([addr, rec]) => [addr, rec.payload] as const);
    }

    get size(): number {
        return this.records.size;
    }

    /**
     * Lifetime ordering, treating an absent `ι` on either side as "same lifetime" so the
     * gate is skipped and behavior falls back to version-only. That keeps a peer running
     * an engine without incarnations interoperable rather than unorderable.
     */
    private order(incoming: IncarnationValue | undefined, stored: IncarnationValue | undefined): number {
        if (incoming === undefined || stored === undefined) return 0;
        return this.incarnation.compare(incoming, stored);
    }
}
