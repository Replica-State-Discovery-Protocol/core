import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Context } from '../../src/domain/context.js';
import { MessageType } from '../../src/domain/message.js';
import { createEngine } from '../../src/engine/Engine.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';
import { defineReducer, type Reducer } from '../../src/reducer/Reducer.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

type V = string[];
const union = (self: string): Aggregator<V, V, Context> => ({
    aggregate: (b) => [...new Set([self, ...b.flat()])].sort(),
});
const reducer = (name: string, self: string): Reducer<unknown, V, Context> =>
    defineReducer<V, V, Context>(name)
        .status((p) => p.setAggregator(union(self)))
        .share((p) => p.setAggregator(union(self)))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({
            translate: (s, prev) => ({ value: s, changed: JSON.stringify(s) !== JSON.stringify(prev) }),
        }) as unknown as Reducer<unknown, V, Context>;

// resync/sweep set well beyond the test window so only the change-driven broadcast fires.
const cfg = {
    debounce: { delayMs: 10, maxWaitMs: 50 },
    ttlMs: 100_000,
    sweepIntervalMs: 100_000,
    resyncIntervalMs: 100_000,
};

const settle = async (clock: FakeClock, engine: { settle(): Promise<void> }) => {
    for (let i = 0; i < 8; i++) {
        clock.advance(20);
        await engine.settle();
        await Promise.resolve();
    }
};

describe('Engine steady-state broadcast coalescing', () => {
    it('emits ONE composite SHARE per cycle even when multiple reducers change', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const r1 = reducer('r1', 'a');
        const r2 = reducer('r2', 'a');
        const engine = createEngine<Context>({
            identity: { address: 'a' },
            slan: new InMemorySlan('a', bus),
            reducers: [r1, r2],
            config: cfg,
            clock,
        });

        // Bare peer that records the SHARE messages 'a' broadcasts.
        const peer = new InMemorySlan('peer', bus);
        let shares = 0;
        peer.onMessage((msg, from) => {
            if (from === 'a' && msg.type === MessageType.Share) shares += 1;
        });
        await peer.init();
        await engine.start();
        await settle(clock, engine);

        const baseline = shares; // the startup DEBATE SHARE
        expect(engine.stateOf(r1)?.value).toEqual(['a']);

        // One incoming SHARE that moves BOTH reducers' Σ in the same cycle.
        await peer.sendTo('a', {
            type: MessageType.Share,
            from: 'peer',
            payloads: {
                r1: { value: ['peer'], version: 1 },
                r2: { value: ['peer'], version: 1 },
            },
        });
        await settle(clock, engine);

        // Both reducers converged...
        expect(engine.stateOf(r1)?.value).toEqual(['a', 'peer']);
        expect(engine.stateOf(r2)?.value).toEqual(['a', 'peer']);
        // ...but the node broadcast exactly ONE composite SHARE for the cycle, not one per reducer.
        expect(shares - baseline).toBe(1);

        await engine.stop();
    });
});
