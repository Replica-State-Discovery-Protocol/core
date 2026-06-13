import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Context } from '../../src/domain/context.js';
import type { WireMessage } from '../../src/domain/message.js';
import { MessageType } from '../../src/domain/message.js';
import { createEngine } from '../../src/engine/Engine.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';
import { defineReducer } from '../../src/reducer/Reducer.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

type V = string[];
// Union of all peer id-lists plus self.
const union = (self: string): Aggregator<V, V, Context> => ({
    aggregate: (batch) => [...new Set([self, ...batch.flat()])].sort(),
});
const reducer = (self: string) =>
    defineReducer<V, V, Context>('members')
        .status((p) => p.setAggregator(union(self)))
        .share((p) => p.setAggregator(union(self)))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({ translate: (state) => ({ value: state, changed: true }) });

const cfg = { debounce: { delayMs: 10, maxWaitMs: 50 }, ttlMs: 10_000, sweepIntervalMs: 1_000 };

describe('Engine DEBATE bootstrap', () => {
    it('broadcasts HELLO on start, then a SHARE after the DEBATE window', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);

        // a passive spy node records what 'a' emits.
        const spy = new InMemorySlan('spy', bus);
        const seen: WireMessage[] = [];
        spy.onMessage((m) => seen.push(m));
        await spy.init();

        const slanA = new InMemorySlan('a', bus);
        const engineA = createEngine<Context>({
            identity: { address: 'a' },
            slan: slanA,
            reducers: [reducer('a')],
            config: cfg,
            clock,
        });
        await engineA.start();

        // HELLO emitted immediately on start.
        expect(seen.map((m) => m.type)).toContain(MessageType.Hello);

        // Simulate a STATUS reply from a neighbour 'b' carrying its view.
        await spy.sendTo('a', {
            type: MessageType.Status,
            from: 'b',
            payloads: { members: { value: ['b'], version: 1 } },
        });

        // Advance past the DEBATE debounce → engine aggregates and broadcasts SHARE.
        clock.advance(50);
        await engineA.settle();

        const share = seen.find((m) => m.type === MessageType.Share);
        expect(share?.payloads?.members?.value).toEqual(['a', 'b']);

        await engineA.stop();
    });
});
