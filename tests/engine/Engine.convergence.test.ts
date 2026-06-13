import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Context } from '../../src/domain/context.js';
import { createEngine } from '../../src/engine/Engine.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';
import { defineReducer, Reducer } from '../../src/reducer/Reducer.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

type V = string[];
const union = (self: string): Aggregator<V, V, Context> => ({
    aggregate: (batch) => [...new Set([self, ...batch.flat()])].sort(),
});
// stateOf() keys on Reducer<unknown, V, Ctx>; S is invariant in the pipeline, so a
// concrete-S Reducer<V, V, Ctx> is widened here for the engine's reducer-handle API.
const membersReducer = (self: string): Reducer<unknown, V, Context> =>
    defineReducer<V, V, Context>('members')
        .status((p) => p.setAggregator(union(self)))
        .share((p) => p.setAggregator(union(self)))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({ translate: (state) => ({ value: state, changed: true }) }) as Reducer<unknown, V, Context>;

const cfg = { debounce: { delayMs: 10, maxWaitMs: 50 }, ttlMs: 10_000, sweepIntervalMs: 1_000 };

const settleAll = async (clock: FakeClock, engines: { settle(): Promise<void> }[]) => {
    for (let i = 0; i < 10; i++) {
        clock.advance(60);
        await Promise.all(engines.map((e) => e.settle()));
        await Promise.resolve();
    }
};

describe('Engine steady-state convergence', () => {
    it('three nodes converge on the full membership set', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const mk = (id: string) => {
            const r = membersReducer(id);
            const engine = createEngine<Context>({
                identity: { address: id },
                slan: new InMemorySlan(id, bus),
                reducers: [r],
                config: cfg,
                clock,
            });
            return { id, r, engine };
        };
        const nodes = [mk('a'), mk('b'), mk('c')];
        await Promise.all(nodes.map((n) => n.engine.start()));

        await settleAll(
            clock,
            nodes.map((n) => n.engine),
        );

        for (const n of nodes) {
            expect(n.engine.stateOf(n.r)?.value).toEqual(['a', 'b', 'c']);
        }
        await Promise.all(nodes.map((n) => n.engine.stop()));
    });

    it('a departing node is removed after CLOSE', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const mk = (id: string) => {
            const r = membersReducer(id);
            const engine = createEngine<Context>({
                identity: { address: id },
                slan: new InMemorySlan(id, bus),
                reducers: [r],
                config: cfg,
                clock,
            });
            return { id, r, engine };
        };
        const nodes = [mk('a'), mk('b'), mk('c')];
        await Promise.all(nodes.map((n) => n.engine.start()));
        await settleAll(
            clock,
            nodes.map((n) => n.engine),
        );

        await nodes[2]!.engine.stop(); // 'c' departs → broadcasts CLOSE
        await settleAll(clock, [nodes[0]!.engine, nodes[1]!.engine]);

        expect(nodes[0]!.engine.stateOf(nodes[0]!.r)?.value).toEqual(['a', 'b']);
        expect(nodes[1]!.engine.stateOf(nodes[1]!.r)?.value).toEqual(['a', 'b']);
        await Promise.all([nodes[0]!.engine.stop(), nodes[1]!.engine.stop()]);
    });

    it('multiple survivors keep their membership after one node departs', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const mk = (id: string) => {
            const r = membersReducer(id);
            const engine = createEngine<Context>({
                identity: { address: id },
                slan: new InMemorySlan(id, bus),
                reducers: [r],
                config: cfg,
                clock,
            });
            return { id, r, engine };
        };
        const nodes = [mk('a'), mk('b'), mk('c'), mk('d')];
        await Promise.all(nodes.map((n) => n.engine.start()));
        await settleAll(
            clock,
            nodes.map((n) => n.engine),
        );
        for (const n of nodes) {
            expect(n.engine.stateOf(n.r)?.value).toEqual(['a', 'b', 'c', 'd']);
        }

        const survivors = [nodes[0]!, nodes[1]!, nodes[2]!];
        await nodes[3]!.engine.stop(); // 'd' departs → broadcasts CLOSE
        await settleAll(
            clock,
            survivors.map((n) => n.engine),
        );

        // The CLOSE invalidate-all rebuild must NOT drop the surviving members.
        for (const n of survivors) {
            expect(n.engine.stateOf(n.r)?.value).toEqual(['a', 'b', 'c']);
        }
        await Promise.all(survivors.map((n) => n.engine.stop()));
    });
});
