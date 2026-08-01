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

describe('FakeClock event-driven stepping', () => {
    it('reports no deadline when nothing is scheduled', () => {
        expect(new FakeClock(1000).nextDeadline()).toBeNull();
    });

    it('reports the earliest pending deadline in absolute time', () => {
        const clock = new FakeClock(1000);
        clock.setTimer(() => undefined, 100);
        clock.setTimer(() => undefined, 50);

        expect(clock.nextDeadline()).toBe(1050);
    });

    it('stops reporting a deadline that was cleared', () => {
        const clock = new FakeClock(0);
        const h = clock.setTimer(() => undefined, 10);
        clock.setTimer(() => undefined, 20);
        clock.clearTimer(h);

        expect(clock.nextDeadline()).toBe(20);
    });

    it('tickNext fires exactly one timer and lands the clock on its deadline', () => {
        const clock = new FakeClock(0);
        const fired: string[] = [];
        clock.setTimer(() => fired.push('a'), 50);
        clock.setTimer(() => fired.push('b'), 100);

        expect(clock.tickNext()).toBe(true);
        expect(fired).toEqual(['a']);
        expect(clock.now()).toBe(50); // exactly the deadline, no over-shoot

        expect(clock.tickNext()).toBe(true);
        expect(fired).toEqual(['a', 'b']);
        expect(clock.now()).toBe(100);
    });

    it('tickNext reports false and leaves time alone when nothing is pending', () => {
        const clock = new FakeClock(500);

        expect(clock.tickNext()).toBe(false);
        expect(clock.now()).toBe(500);
    });

    it('breaks deadline ties by insertion order, so replays are identical', () => {
        const clock = new FakeClock(0);
        const fired: string[] = [];
        clock.setTimer(() => fired.push('first'), 10);
        clock.setTimer(() => fired.push('second'), 10);
        clock.setTimer(() => fired.push('third'), 10);

        while (clock.tickNext());
        expect(fired).toEqual(['first', 'second', 'third']);
    });

    it('picks up a timer scheduled from within a tick on the following tick', () => {
        const clock = new FakeClock(0);
        const fired: string[] = [];
        clock.setTimer(() => {
            fired.push('outer');
            clock.setTimer(() => fired.push('inner'), 5);
        }, 10);

        expect(clock.tickNext()).toBe(true);
        expect(fired).toEqual(['outer']);
        expect(clock.nextDeadline()).toBe(15); // scheduled relative to the tick instant

        expect(clock.tickNext()).toBe(true);
        expect(fired).toEqual(['outer', 'inner']);
        expect(clock.now()).toBe(15);
    });
});
