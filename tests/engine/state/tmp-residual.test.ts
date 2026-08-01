import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../../src/clock/Clock.js';
import { MemoryMap } from '../../../src/engine/state/MemoryMap.js';
import { TimestampIncarnation } from '../../../src/incarnation/Incarnation.js';

describe('residual exposure of the pinning mechanism', () => {
    it('a sender that stamps no ι can still pin a stale record forever', () => {
        const m = new MemoryMap<string>(new TimestampIncarnation(new FakeClock(0)));
        m.update('j', 'pre-crash', 2, 0); // no inc — a peer on an older engine

        for (let t = 500; t <= 100_000; t += 500) {
            m.update('j', 'post-recovery', 2, t); // still no inc, differing payload
            expect(m.sweepExpired(t, 5_000)).toEqual([]);
        }
        expect(m.snapshot()).toEqual([['j', 'pre-crash']]);
    });

    it('two lifetimes sharing one clock tick collide and pin as well', () => {
        const clock = new FakeClock(1_000);
        const first = new TimestampIncarnation(clock).value;
        const second = new TimestampIncarnation(clock).value; // same tick → same ι
        expect(second).toBe(first);

        const m = new MemoryMap<string>(new TimestampIncarnation(clock));
        m.update('j', 'pre-crash', 2, 0, first);
        for (let t = 500; t <= 100_000; t += 500) {
            m.update('j', 'post-recovery', 2, t, second);
            expect(m.sweepExpired(t, 5_000)).toEqual([]);
        }
        expect(m.snapshot()).toEqual([['j', 'pre-crash']]);
    });

    it('but a genuinely newer ι displaces it immediately', () => {
        const m = new MemoryMap<string>(new TimestampIncarnation(new FakeClock(0)));
        m.update('j', 'pre-crash', 2, 0, 1_000);
        expect(m.update('j', 'post-recovery', 2, 500, 4_000)).toBe(true);
    });
});
