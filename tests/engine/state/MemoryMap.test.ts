import { describe, expect, it } from 'vitest';

import { MemoryMap } from '../../../src/engine/state/MemoryMap.js';

describe('MemoryMap', () => {
    it('updates and snapshots non-evicted peers', () => {
        const m = new MemoryMap<string>();
        expect(m.update('a', 'x', 1, 1000)).toBe(true);
        expect(m.update('b', 'y', 1, 1000)).toBe(true);
        expect([...m.snapshot()]).toEqual([
            ['a', 'x'],
            ['b', 'y'],
        ]);
    });

    it('ignores stale or equal versions (no state change)', () => {
        const m = new MemoryMap<string>();
        m.update('a', 'x', 5, 1000);
        expect(m.update('a', 'OLD', 4, 1001)).toBe(false);
        expect(m.update('a', 'SAME', 5, 1002)).toBe(false);
        expect(m.update('a', 'NEW', 6, 1003)).toBe(true);
        expect([...m.snapshot()]).toEqual([['a', 'NEW']]);
    });

    it('refreshes liveness on an equal-version re-broadcast (keeps payload/version)', () => {
        const m = new MemoryMap<string>();
        m.update('a', 'x', 5, 1000);
        // A later equal-version SHARE (periodic resync) refreshes lastSeenAt and
        // keeps the stored payload/version, but reports no STATE change.
        expect(m.update('a', 'x', 5, 5000)).toBe(false);
        expect([...m.snapshot()]).toEqual([['a', 'x']]);
        // The refresh must let the peer survive a sweep it would otherwise fail:
        // ttl=600 at t=5500 would evict the original lastSeenAt (1000), but the
        // equal-version refresh moved it to 5000, so it survives.
        expect(m.sweepExpired(5500, 600)).toEqual([]);
        expect([...m.snapshot()]).toEqual([['a', 'x']]);
        // It is still evicted once it genuinely goes silent past the TTL.
        expect(m.sweepExpired(6000, 600).sort()).toEqual(['a']);
    });

    it('does NOT refresh liveness on a stale (lower-version) update', () => {
        const m = new MemoryMap<string>();
        m.update('a', 'x', 5, 1000);
        // A stale, lower-version SHARE is fully ignored — no liveness refresh.
        expect(m.update('a', 'OLD', 4, 5000)).toBe(false);
        expect(m.sweepExpired(1700, 600).sort()).toEqual(['a']);
    });

    it('evicts explicitly and by TTL', () => {
        const m = new MemoryMap<string>();
        m.update('a', 'x', 1, 1000);
        m.update('b', 'y', 1, 1000);
        expect(m.evict('a')).toBe(true);
        expect(m.evict('a')).toBe(false);
        expect([...m.snapshot()]).toEqual([['b', 'y']]);

        m.update('c', 'z', 1, 2000);
        // at t=2500, ttl=600: b (lastSeen 1000) expires, c (2000) survives
        expect(m.sweepExpired(2500, 600).sort()).toEqual(['b']);
        expect([...m.snapshot()]).toEqual([['c', 'z']]);
    });
});
