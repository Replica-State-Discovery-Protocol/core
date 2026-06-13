// src/engine/memory/MemoryMap.ts
import type { Address } from '../../domain/address.js';
import type { PeerRecord } from '../../domain/peer.js';

export class MemoryMap<P> {
    private readonly records = new Map<Address, PeerRecord<P>>();

    /** Version-gated. Returns true if the map changed. */
    update(addr: Address, payload: P, version: number, now: number): boolean {
        const existing = this.records.get(addr);
        if (existing && version <= existing.version) return false;
        this.records.set(addr, { payload, version, lastSeenAt: now });
        return true;
    }

    evict(addr: Address): boolean {
        return this.records.delete(addr);
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
