import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../../src/clock/Clock.js';
import { type Sweepable, TtlSweeper } from '../../../src/engine/internal/TtlSweeper.js';

// Records sweep calls and returns a queued "evicted?" verdict per tick.
class FakeTarget implements Sweepable {
    readonly calls: { now: number; ttlMs: number }[] = [];
    private verdicts: boolean[] = [];

    queue(...evicted: boolean[]): void {
        this.verdicts.push(...evicted);
    }
    sweepExpired(now: number, ttlMs: number): boolean {
        this.calls.push({ now, ttlMs });
        return this.verdicts.shift() ?? false;
    }
}

describe('TtlSweeper', () => {
    it('sweeps once per interval with the current clock time and configured ttl', () => {
        const clock = new FakeClock(0);
        const target = new FakeTarget();
        const sweeper = new TtlSweeper(clock, target, 1000, 100, () => undefined);

        sweeper.start();
        expect(target.calls).toHaveLength(0); // nothing until the interval elapses

        clock.advance(100);
        clock.advance(100);
        expect(target.calls).toEqual([
            { now: 100, ttlMs: 1000 },
            { now: 200, ttlMs: 1000 },
        ]);

        sweeper.stop();
    });

    it('fires onEvicted only when a sweep actually removed something', () => {
        const clock = new FakeClock(0);
        const target = new FakeTarget();
        let evictions = 0;
        const sweeper = new TtlSweeper(clock, target, 1000, 100, () => {
            evictions += 1;
        });
        target.queue(false, true, false); // 1st tick: none, 2nd: evicted, 3rd: none

        sweeper.start();
        clock.advance(100);
        clock.advance(100);
        clock.advance(100);

        expect(target.calls).toHaveLength(3);
        expect(evictions).toBe(1);
        sweeper.stop();
    });

    it('stop() halts further sweeps', () => {
        const clock = new FakeClock(0);
        const target = new FakeTarget();
        const sweeper = new TtlSweeper(clock, target, 1000, 100, () => undefined);

        sweeper.start();
        clock.advance(100);
        expect(target.calls).toHaveLength(1);

        sweeper.stop();
        clock.advance(1000);
        expect(target.calls).toHaveLength(1); // no more after stop
    });
});
