import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import { StaticIncarnation, TimestampIncarnation } from '../../src/incarnation/Incarnation.js';

describe('TimestampIncarnation', () => {
    it('reads its value from the clock at construction', () => {
        expect(new TimestampIncarnation(new FakeClock(1_000)).value).toBe(1_000);
    });

    it('gives a later-constructed instance a strictly greater value', () => {
        const clock = new FakeClock(1_000);
        const before = new TimestampIncarnation(clock);
        clock.advance(1);
        const after = new TimestampIncarnation(clock);

        expect(after.compare(after.value, before.value)).toBeGreaterThan(0);
    });

    it('orders wire values numerically', () => {
        const inc = new TimestampIncarnation(new FakeClock(0));

        expect(inc.compare(2, 1)).toBeGreaterThan(0);
        expect(inc.compare(1, 2)).toBeLessThan(0);
        expect(inc.compare(1, 1)).toBe(0);
    });
});

describe('StaticIncarnation', () => {
    it('exposes the value it was given', () => {
        expect(new StaticIncarnation(0).value).toBe(0);
    });

    it('never orders one lifetime above another, so the gate degenerates to version-only', () => {
        // The §7.3 "stock protocol" arm: no lifetime ordering exists, so eq. (15) always
        // falls through to the eq. (3) version gates — the pre-amendment behavior.
        const inc = new StaticIncarnation(0);

        expect(inc.compare(5, 1)).toBe(0);
        expect(inc.compare(1, 5)).toBe(0);
    });
});
