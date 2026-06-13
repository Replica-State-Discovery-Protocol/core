import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Context } from '../../src/domain/context.js';
import { MessageType } from '../../src/domain/message.js';
import { createEngine } from '../../src/engine/Engine.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';
import { defineReducer, Reducer } from '../../src/reducer/Reducer.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

type V = string[];
const union = (self: string): Aggregator<V, V, Context> => ({
    aggregate: (b) => [...new Set([self, ...b.flat()])].sort(),
});
// stateOf() keys on Reducer<unknown, V, Ctx>; S is invariant in the pipeline, so a
// concrete-S Reducer<V, V, Ctx> is widened here for the engine's reducer-handle API.
const r = (self: string): Reducer<unknown, V, Context> =>
    defineReducer<V, V, Context>('members')
        .status((p) => p.setAggregator(union(self)))
        .share((p) => p.setAggregator(union(self)))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({ translate: (state) => ({ value: state, changed: true }) }) as Reducer<unknown, V, Context>;
const cfg = { debounce: { delayMs: 10, maxWaitMs: 50 }, ttlMs: 100, sweepIntervalMs: 20, resyncIntervalMs: 30 };

describe('Engine edge cases', () => {
    it('an isolated node bootstraps to just itself at D_max', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const red = r('solo');
        const engine = createEngine<Context>({
            identity: { address: 'solo' },
            slan: new InMemorySlan('solo', bus),
            reducers: [red],
            config: cfg,
            clock,
        });
        await engine.start();
        clock.advance(50); // D_max
        await engine.settle();
        expect(engine.stateOf(red)?.value).toEqual(['solo']);
        await engine.stop();
    });

    it('drops a silent peer after TTL', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const red = r('a');
        const engine = createEngine<Context>({
            identity: { address: 'a' },
            slan: new InMemorySlan('a', bus),
            reducers: [red],
            config: cfg,
            clock,
        });
        const peer = new InMemorySlan('ghost', bus);
        await peer.init();
        await engine.start();
        clock.advance(50);
        await engine.settle();

        // ghost sends one SHARE then goes silent
        await peer.sendTo('a', {
            type: MessageType.Share,
            from: 'ghost',
            payloads: { members: { value: ['ghost'], version: 1 } },
        });
        clock.advance(20);
        await engine.settle();
        expect(engine.stateOf(red)?.value).toContain('ghost');

        // advance well past TTL → swept
        clock.advance(200);
        await engine.settle();
        expect(engine.stateOf(red)?.value).not.toContain('ghost');
        await engine.stop();
    });
});
