import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../../src/clock/Clock.js';
import { Phase } from '../../../src/engine/schedule/Fsm.js';
import { type PhaseHandlers, PhaseScheduler } from '../../../src/engine/schedule/PhaseScheduler.js';

const cfg = { delayMs: 10, maxWaitMs: 50 };

/** A gate whose handler suspends until released — to hold a phase "in flight". */
const gate = () => {
    let release: () => void = () => undefined;
    const promise = new Promise<void>((r) => (release = r));
    return { promise, release };
};

const noop = (): Promise<void> => Promise.resolve();

describe('PhaseScheduler', () => {
    it('bootstraps INITIAL → DEBATE, runs the handler, and returns to IDLE', async () => {
        const clock = new FakeClock(0);
        const log: Phase[] = [];
        const handlers: PhaseHandlers = {
            [Phase.DEBATE]: () => {
                log.push(Phase.DEBATE);
                return Promise.resolve();
            },
            [Phase.SHARE]: noop,
            [Phase.CLOSE]: noop,
        };
        const s = new PhaseScheduler({ clock, debounce: cfg, handlers });

        expect(s.phase).toBe(Phase.INITIAL);
        s.request(Phase.DEBATE);
        clock.advance(10);
        await s.settle();

        expect(log).toEqual([Phase.DEBATE]);
        expect(s.phase).toBe(Phase.IDLE);
    });

    it('holds a SHARE/CLOSE requested before the bootstrap DEBATE (no illegal INITIAL transition)', async () => {
        const clock = new FakeClock(0);
        const order: Phase[] = [];
        const handlers: PhaseHandlers = {
            [Phase.DEBATE]: () => {
                order.push(Phase.DEBATE);
                return Promise.resolve();
            },
            [Phase.SHARE]: () => {
                order.push(Phase.SHARE);
                return Promise.resolve();
            },
            [Phase.CLOSE]: noop,
        };
        const s = new PhaseScheduler({ clock, debounce: cfg, handlers });

        s.request(Phase.SHARE); // arrives first, but INITIAL → SHARE is illegal
        clock.advance(10);
        await s.settle();
        expect(order).toEqual([]); // SHARE held — still INITIAL, nothing ran
        expect(s.phase).toBe(Phase.INITIAL);

        s.request(Phase.DEBATE);
        clock.advance(10);
        await s.settle();
        // DEBATE bootstraps, then the deferred SHARE runs from IDLE.
        expect(order).toEqual([Phase.DEBATE, Phase.SHARE]);
        expect(s.phase).toBe(Phase.IDLE);
    });

    it('defers a SHARE requested mid-DEBATE until DEBATE completes', async () => {
        const clock = new FakeClock(0);
        const order: string[] = [];
        const g = gate();
        const handlers: PhaseHandlers = {
            [Phase.DEBATE]: async () => {
                order.push('debate:start');
                await g.promise;
                order.push('debate:end');
            },
            [Phase.SHARE]: () => {
                order.push('share');
                return Promise.resolve();
            },
            [Phase.CLOSE]: noop,
        };
        const s = new PhaseScheduler({ clock, debounce: cfg, handlers });

        s.request(Phase.DEBATE);
        clock.advance(10); // DEBATE fires, handler suspends on the gate
        expect(order).toEqual(['debate:start']);
        expect(s.phase).toBe(Phase.DEBATE);

        s.request(Phase.SHARE);
        clock.advance(10); // SHARE window fires → queued (drain busy), must NOT run
        await Promise.resolve();
        expect(order).toEqual(['debate:start']);
        expect(s.phase).toBe(Phase.DEBATE);

        g.release();
        await s.settle();
        expect(order).toEqual(['debate:start', 'debate:end', 'share']);
        expect(s.phase).toBe(Phase.IDLE);
    });

    it('deduplicates: a repeat request while scheduled does not double-run; isScheduled tracks it', async () => {
        const clock = new FakeClock(0);
        let runs = 0;
        const handlers: PhaseHandlers = {
            [Phase.DEBATE]: () => {
                runs += 1;
                return Promise.resolve();
            },
            [Phase.SHARE]: noop,
            [Phase.CLOSE]: noop,
        };
        const s = new PhaseScheduler({ clock, debounce: cfg, handlers });

        expect(s.isScheduled(Phase.DEBATE)).toBe(false);
        s.request(Phase.DEBATE);
        expect(s.isScheduled(Phase.DEBATE)).toBe(true); // window armed
        s.request(Phase.DEBATE); // coalesced into the same window
        clock.advance(10);
        await s.settle();

        expect(runs).toBe(1);
        expect(s.isScheduled(Phase.DEBATE)).toBe(false);
    });

    it('reports isScheduled(true) for a phase queued behind an in-flight phase (resync dedup)', async () => {
        const clock = new FakeClock(0);
        const g = gate();
        const handlers: PhaseHandlers = {
            [Phase.DEBATE]: () => g.promise,
            [Phase.SHARE]: noop,
            [Phase.CLOSE]: noop,
        };
        const s = new PhaseScheduler({ clock, debounce: cfg, handlers });

        s.request(Phase.DEBATE);
        clock.advance(10); // DEBATE in flight (suspended on gate)
        expect(s.phase).toBe(Phase.DEBATE);
        expect(s.isScheduled(Phase.DEBATE)).toBe(true); // executing → a resync would skip

        g.release();
        await s.settle();
        expect(s.isScheduled(Phase.DEBATE)).toBe(false);
    });

    it('runs a CLOSE phase and returns to IDLE', async () => {
        const clock = new FakeClock(0);
        const log: Phase[] = [];
        const handlers: PhaseHandlers = {
            [Phase.DEBATE]: () => {
                log.push(Phase.DEBATE);
                return Promise.resolve();
            },
            [Phase.SHARE]: noop,
            [Phase.CLOSE]: () => {
                log.push(Phase.CLOSE);
                return Promise.resolve();
            },
        };
        const s = new PhaseScheduler({ clock, debounce: cfg, handlers });

        // Bootstrap first (CLOSE is only legal from IDLE).
        s.request(Phase.DEBATE);
        clock.advance(10);
        await s.settle();

        s.request(Phase.CLOSE);
        clock.advance(10);
        await s.settle();
        expect(log).toEqual([Phase.DEBATE, Phase.CLOSE]);
        expect(s.phase).toBe(Phase.IDLE);
    });

    it('stop() cancels armed windows so nothing fires afterward', async () => {
        const clock = new FakeClock(0);
        let runs = 0;
        const handlers: PhaseHandlers = {
            [Phase.DEBATE]: () => {
                runs += 1;
                return Promise.resolve();
            },
            [Phase.SHARE]: noop,
            [Phase.CLOSE]: noop,
        };
        const s = new PhaseScheduler({ clock, debounce: cfg, handlers });

        s.request(Phase.DEBATE);
        s.stop();
        clock.advance(50);
        await s.settle();
        expect(runs).toBe(0);
    });
});
