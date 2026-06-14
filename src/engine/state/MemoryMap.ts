import type { Address } from '../../domain/address.js';
import type { PeerRecord } from '../../domain/peer.js';

export class MemoryMap<P> {
    private readonly records = new Map<Address, PeerRecord<P>>();

    /**
     * Version-gated. Returns true iff the stored VALUE changed.
     *
     * - `version < existing.version` → stale, ignored entirely (no liveness refresh).
     * - `version === existing.version` → a periodic re-broadcast (resync): refresh
     *   `lastSeenAt` so TTL keeps the live peer, but keep payload/version and report
     *   no state change (`false`). Liveness is refreshed without a recompute storm.
     * - `version > existing.version` (or no existing) → store the new payload, refresh
     *   liveness, report a state change (`true`).
     */
    update(addr: Address, payload: P, version: number, now: number): boolean {
        const existing = this.records.get(addr);
        if (existing) {
            if (version < existing.version) return false;
            if (version === existing.version) {
                existing.lastSeenAt = now;
                return false;
            }
        }
        this.records.set(addr, { payload, version, lastSeenAt: now });
        return true;
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

    snapshot(): readonly (readonly [Address, P])[] {
        return [...this.records.entries()].map(([addr, rec]) => [addr, rec.payload] as const);
    }

    get size(): number {
        return this.records.size;
    }
}
