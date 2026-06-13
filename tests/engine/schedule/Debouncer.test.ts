// tests/engine/schedule/Debouncer.test.ts
import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../../src/clock/Clock.js';
import { Debouncer } from '../../../src/engine/schedule/Debouncer.js';

describe('Debouncer', () => {
    it('coalesces bursts and fires after δ of quiet', () => {
        const clock = new FakeClock(0);
        let fires = 0;
        const d = new Debouncer(clock, { delayMs: 10, maxWaitMs: 100 }, () => fires++);
        d.notifyChange(); // t=0, scheduled ~10
        clock.advance(5);
        d.notifyChange(); // reschedules to 15
        clock.advance(9); // t=14, not yet
        expect(fires).toBe(0);
        clock.advance(1); // t=15 → fire
        expect(fires).toBe(1);
    });

    it('never delays beyond D_max since the first change', () => {
        const clock = new FakeClock(0);
        let fires = 0;
        const d = new Debouncer(clock, { delayMs: 10, maxWaitMs: 25 }, () => fires++);
        d.notifyChange(); // first change at t=0 → hard cap t=25
        for (let i = 0; i < 5; i++) {
            clock.advance(8);
            d.notifyChange(); // keep pushing the δ window
        }
        expect(fires).toBe(1); // fired at t=25 despite continuous changes
    });

    it('starts a fresh window after firing', () => {
        const clock = new FakeClock(0);
        let fires = 0;
        const d = new Debouncer(clock, { delayMs: 10, maxWaitMs: 100 }, () => fires++);
        d.notifyChange();
        clock.advance(10);
        expect(fires).toBe(1);
        d.notifyChange();
        clock.advance(10);
        expect(fires).toBe(2);
    });
});
