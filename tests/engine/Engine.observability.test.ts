// The two read-only seams §7 of the paper measures through: eviction events (eq. 7 is
// defined over `dom Σ`, which the derived view lags by the debounce window) and direct
// `Σ` inspection.
import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Context } from '../../src/domain/context.js';
import type { PeerEviction } from '../../src/domain/peer.js';
import { createEngine } from '../../src/engine/Engine.js';
import { StaticIncarnation } from '../../src/incarnation/Incarnation.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';
import { defineReducer, Reducer } from '../../src/reducer/Reducer.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

type V = string[];
const union = (self: string): Aggregator<V, V, Context> => ({
    aggregate: (batch) => [...new Set([self, ...batch.flat()])].sort(),
});
const membersReducer = (self: string): Reducer<unknown, V, Context> =>
    defineReducer<V, V, Context>('members')
        .status((p) => p.setAggregator(union(self)))
        .share((p) => p.setAggregator(union(self)))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({
            translate: (s, prev) => ({ value: s, changed: JSON.stringify(s) !== JSON.stringify(prev) }),
        }) as Reducer<unknown, V, Context>;

const cfg = {
    debounce: { delayMs: 10, maxWaitMs: 50 },
    ttlMs: 1_000,
    sweepIntervalMs: 100,
    resyncIntervalMs: 400,
};

/**
 * Each node gets its own clock, so a crash can be modelled by simply not advancing it —
 * the node stops emitting without the graceful `stop()` that would broadcast a CLOSE.
 */
const setup = () => {
    const bus = new InMemoryBus();
    const mk = (id: string) => {
        const clock = new FakeClock(0);
        const reducer = membersReducer(id);
        const engine = createEngine<Context>({
            identity: { address: id },
            slan: new InMemorySlan(id, bus),
            reducers: [reducer],
            config: cfg,
            clock,
            incarnation: new StaticIncarnation(id), // deterministic, clock-independent
        });
        return { id, clock, reducer, engine };
    };
    return { bus, a: mk('a'), b: mk('b') };
};

const settle = async (
    nodes: { clock: FakeClock; engine: { settle(): Promise<void> } }[],
    rounds = 10,
): Promise<void> => {
    for (let i = 0; i < rounds; i++) {
        for (const n of nodes) n.clock.advance(60);
        await Promise.all(nodes.map((n) => n.engine.settle()));
        await Promise.resolve();
    }
};

describe('Engine.sigmaOf', () => {
    it('is empty before any peer is known', () => {
        const { a } = setup();
        expect(a.engine.sigmaOf(a.reducer)).toEqual([]);
    });

    it('exposes a peer’s Σ record once its SHARE is admitted', async () => {
        const { a, b } = setup();
        await a.engine.start();
        await b.engine.start();
        await settle([a, b]);

        const sigma = a.engine.sigmaOf(a.reducer);
        expect(sigma.map((s) => s.addr)).toEqual(['b']);
        expect(sigma[0]?.version).toBeGreaterThan(0);
        expect(sigma[0]?.inc).toBe('b'); // the sender's ι, not ours

        await a.engine.stop();
        await b.engine.stop();
    });
});

describe('Engine.onEvicted', () => {
    it('reports the peer and reducer a TTL sweep removed, and stops on unsubscribe', async () => {
        const { bus, a, b } = setup();
        const seen: PeerEviction[][] = [];
        const unsubscribe = a.engine.onEvicted((e) => seen.push(e));

        await a.engine.start();
        await b.engine.start();
        await settle([a, b]);
        expect(a.engine.sigmaOf(a.reducer).map((s) => s.addr)).toEqual(['b']);

        // Crash-stop `b`: unregister it so nothing reaches it, and freeze its clock so it
        // emits nothing. No CLOSE is sent, so only the TTL sweep can remove it from Σ_a.
        bus.unregister('b');
        await settle([a], 40);

        expect(seen).toEqual([[{ reducer: 'members', addrs: ['b'] }]]);
        expect(a.engine.sigmaOf(a.reducer)).toEqual([]);

        unsubscribe();
        seen.length = 0;
        await settle([a], 40);
        expect(seen).toEqual([]);

        await a.engine.stop();
    });
});
