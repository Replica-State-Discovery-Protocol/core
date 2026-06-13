// tests/clock/Clock.test.ts
import { describe, expect, it, vi } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';

describe('FakeClock', () => {
    it('fires timers only once their delay has elapsed via advance()', () => {
        const clock = new FakeClock(1000);
        const fired: string[] = [];
        clock.setTimer(() => fired.push('a'), 50);
        clock.setTimer(() => fired.push('b'), 100);

        clock.advance(49);
        expect(fired).toEqual([]);
        clock.advance(1); // now 1050 → 'a' due
        expect(fired).toEqual(['a']);
        expect(clock.now()).toBe(1050);
        clock.advance(50); // now 1100 → 'b' due
        expect(fired).toEqual(['a', 'b']);
    });

    it('clearTimer prevents firing', () => {
        const clock = new FakeClock(0);
        const fn = vi.fn();
        const h = clock.setTimer(fn, 10);
        clock.clearTimer(h);
        clock.advance(100);
        expect(fn).not.toHaveBeenCalled();
    });
});
