import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../../src/clock/Clock.js';
import type { PeerEviction } from '../../../src/domain/peer.js';
import { type Sweepable, TtlSweeper } from '../../../src/engine/schedule/TtlSweeper.js';

// Records sweep calls and returns a queued eviction report per tick.
class FakeTarget implements Sweepable {
    readonly calls: { now: number; ttlMs: number }[] = [];
    private verdicts: PeerEviction[][] = [];

    queue(...evicted: PeerEviction[][]): void {
        this.verdicts.push(...evicted);
    }
    sweepExpired(now: number, ttlMs: number): PeerEviction[] {
        this.calls.push({ now, ttlMs });
        return this.verdicts.shift() ?? [];
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

    it('fires onEvicted only when a sweep actually removed something, passing what was lost', () => {
        const clock = new FakeClock(0);
        const target = new FakeTarget();
        const seen: PeerEviction[][] = [];
        const sweeper = new TtlSweeper(clock, target, 1000, 100, (e) => seen.push(e));
        const lost: PeerEviction[] = [{ reducer: 'members', addrs: ['b', 'c'] }];
        target.queue([], lost, []); // 1st tick: none, 2nd: evicted, 3rd: none

        sweeper.start();
        clock.advance(100);
        clock.advance(100);
        clock.advance(100);

        expect(target.calls).toHaveLength(3);
        expect(seen).toEqual([lost]);
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
