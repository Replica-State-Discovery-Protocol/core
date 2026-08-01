import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../../src/clock/Clock.js';
import { MemoryMap } from '../../../src/engine/state/MemoryMap.js';
import { TimestampIncarnation } from '../../../src/incarnation/Incarnation.js';

/** Ordinary numeric ι ordering; tests that pass no ι exercise the pre-amendment gates. */
const mk = (): MemoryMap<string> => new MemoryMap<string>(new TimestampIncarnation(new FakeClock(0)));

describe('MemoryMap', () => {
    it('updates and snapshots non-evicted peers', () => {
        const m = mk();
        expect(m.update('a', 'x', 1, 1000)).toBe(true);
        expect(m.update('b', 'y', 1, 1000)).toBe(true);
        expect([...m.snapshot()]).toEqual([
            ['a', 'x'],
            ['b', 'y'],
        ]);
    });

    it('ignores stale or equal versions (no state change)', () => {
        const m = mk();
        m.update('a', 'x', 5, 1000);
        expect(m.update('a', 'OLD', 4, 1001)).toBe(false);
        expect(m.update('a', 'SAME', 5, 1002)).toBe(false);
        expect(m.update('a', 'NEW', 6, 1003)).toBe(true);
        expect([...m.snapshot()]).toEqual([['a', 'NEW']]);
    });

    it('refreshes liveness on an equal-version re-broadcast (keeps payload/version)', () => {
        const m = mk();
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
        const m = mk();
        m.update('a', 'x', 5, 1000);
        // A stale, lower-version SHARE is fully ignored — no liveness refresh.
        expect(m.update('a', 'OLD', 4, 5000)).toBe(false);
        expect(m.sweepExpired(1700, 600).sort()).toEqual(['a']);
    });

    it('evicts explicitly and by TTL', () => {
        const m = mk();
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

describe('MemoryMap incarnation gate (eq. 15)', () => {
    it('adopts a newer incarnation even when its version is LOWER', () => {
        const m = mk();
        m.update('a', 'pre-crash', 5, 1000, 100);

        // A restarted node re-counts its versions from zero; ι is what says so.
        expect(m.update('a', 'post-recovery', 1, 2000, 200)).toBe(true);
        expect([...m.snapshot()]).toEqual([['a', 'post-recovery']]);
    });

    it('refreshes liveness when it adopts a newer incarnation', () => {
        const m = mk();
        m.update('a', 'pre-crash', 5, 1000, 100);
        m.update('a', 'post-recovery', 1, 5000, 200);

        expect(m.sweepExpired(5500, 600)).toEqual([]);
    });

    it('ignores an older incarnation even when its version is HIGHER, without refreshing liveness', () => {
        const m = mk();
        m.update('a', 'current', 1, 1000, 200);

        // A delayed message from a dead lifetime must not be adopted, and must not
        // postpone the TTL of the record it is impersonating.
        expect(m.update('a', 'from the grave', 99, 5000, 100)).toBe(false);
        expect([...m.snapshot()]).toEqual([['a', 'current']]);
        expect(m.sweepExpired(1700, 600).sort()).toEqual(['a']);
    });

    it('falls through to the version gates within one incarnation', () => {
        const m = mk();
        m.update('a', 'x', 5, 1000, 7);

        expect(m.update('a', 'OLD', 4, 1001, 7)).toBe(false);
        expect(m.update('a', 'SAME', 5, 1002, 7)).toBe(false);
        expect(m.update('a', 'NEW', 6, 1003, 7)).toBe(true);
        expect([...m.snapshot()]).toEqual([['a', 'NEW']]);
    });

    it('skips the gate when either side carries no ι, so a legacy peer behaves as before', () => {
        const m = mk();
        m.update('a', 'x', 5, 1000); // stored without ι

        expect(m.update('a', 'ignored', 4, 1001, 999)).toBe(false); // version gate still rules
        expect(m.update('a', 'NEW', 6, 1002, 999)).toBe(true);
    });

    it('REGRESSION: a restarted node is not shadowed by its own stale record', () => {
        // The traced failure: peers hold (x_pre, v_pre = 2); the recovered node re-derives
        // the same content in the same number of steps and re-emits version 2, whose
        // equal-version SHAREs used to refresh τ forever and pin the dead payload.
        const m = mk();
        m.update('j', 'pre-crash value', 2, 0, 1_000);

        expect(m.update('j', 'post-recovery value', 1, 4_000, 4_000)).toBe(true);
        expect([...m.snapshot()]).toEqual([['j', 'post-recovery value']]);
    });
});

describe('MemoryMap.isOlderIncarnation', () => {
    it('reports false for a peer it has never seen', () => {
        expect(mk().isOlderIncarnation('nobody', 1)).toBe(false);
    });

    it('reports true only for an incarnation strictly older than the stored one', () => {
        const m = mk();
        m.update('a', 'x', 1, 1000, 100);

        expect(m.isOlderIncarnation('a', 99)).toBe(true);
        expect(m.isOlderIncarnation('a', 100)).toBe(false);
        expect(m.isOlderIncarnation('a', 101)).toBe(false);
    });

    it('reports false when either side carries no ι', () => {
        const m = mk();
        m.update('a', 'x', 1, 1000); // stored without ι
        expect(m.isOlderIncarnation('a', 1)).toBe(false);

        const n = mk();
        n.update('b', 'y', 1, 1000, 100);
        expect(n.isOlderIncarnation('b', undefined)).toBe(false);
    });
});
