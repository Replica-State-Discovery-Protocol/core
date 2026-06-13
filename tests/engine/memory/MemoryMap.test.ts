// tests/engine/memory/MemoryMap.test.ts
import { describe, expect, it } from 'vitest';

import { MemoryMap } from '../../../src/engine/memory/MemoryMap.js';

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

    it('ignores stale or equal versions', () => {
        const m = new MemoryMap<string>();
        m.update('a', 'x', 5, 1000);
        expect(m.update('a', 'OLD', 4, 1001)).toBe(false);
        expect(m.update('a', 'SAME', 5, 1002)).toBe(false);
        expect(m.update('a', 'NEW', 6, 1003)).toBe(true);
        expect([...m.snapshot()]).toEqual([['a', 'NEW']]);
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
