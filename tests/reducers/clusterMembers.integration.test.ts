import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Context } from '../../src/domain/context.js';
import { createEngine, type Engine } from '../../src/engine/Engine.js';
import { clusterMembersReducer } from '../../src/reducers/clusterMembers.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

// End-to-end membership convergence over the in-process bus: real engines, real
// reducer, deterministic FakeClock. Each node runs clusterMembersReducer and learns the
// full live set purely by exchanging HELLO/STATUS/SHARE/CLOSE — no shared state.

const cfg = {
    debounce: { delayMs: 10, maxWaitMs: 50 },
    ttlMs: 10_000,
    sweepIntervalMs: 500,
    resyncIntervalMs: 200,
};

interface Node {
    id: string;
    engine: Engine<Context>;
    reducer: ReturnType<typeof clusterMembersReducer>;
}

const makeNode = (id: string, bus: InMemoryBus, clock: FakeClock): Node => {
    const reducer = clusterMembersReducer(id);
    const engine = createEngine<Context>({
        identity: { address: id },
        slan: new InMemorySlan(id, bus),
        reducers: [reducer],
        config: cfg,
        clock,
    });
    return { id, engine, reducer };
};

/** Advance the clock and settle every node repeatedly, letting gossip rounds play out. */
const converge = async (clock: FakeClock, nodes: Node[], rounds = 25): Promise<void> => {
    for (let i = 0; i < rounds; i++) {
        clock.advance(60);
        await Promise.all(nodes.map((n) => n.engine.settle()));
        await Promise.resolve();
    }
};

/** A node's own view of the membership. */
const viewOf = (n: Node): string[] | null => n.engine.stateOf(n.reducer)?.value ?? null;

describe('clusterMembers integration — 5-node membership convergence', () => {
    it('five nodes independently converge on the full, valid membership set', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const ids = ['a', 'b', 'c', 'd', 'e'];
        const nodes = ids.map((id) => makeNode(id, bus, clock));

        await Promise.all(nodes.map((n) => n.engine.start()));
        await converge(clock, nodes);

        // Valid converged state: every node agrees, and the agreed set is EXACTLY the
        // live membership — no node missing, no phantom id.
        for (const n of nodes) {
            expect(viewOf(n)).toEqual(ids);
        }

        await Promise.all(nodes.map((n) => n.engine.stop()));
    });

    it('survivors converge on the reduced set after a node departs (CLOSE)', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const ids = ['a', 'b', 'c', 'd', 'e'];
        const nodes = ids.map((id) => makeNode(id, bus, clock));

        await Promise.all(nodes.map((n) => n.engine.start()));
        await converge(clock, nodes);
        for (const n of nodes) expect(viewOf(n)).toEqual(ids);

        // 'c' leaves and broadcasts CLOSE; the four survivors must agree on a, b, d, e.
        const departing = nodes.find((n) => n.id === 'c')!;
        const survivors = nodes.filter((n) => n.id !== 'c');
        await departing.engine.stop();
        await converge(clock, survivors);

        for (const n of survivors) {
            expect(viewOf(n)).toEqual(['a', 'b', 'd', 'e']);
        }

        await Promise.all(survivors.map((n) => n.engine.stop()));
    });

    it('a late joiner is folded in: all six converge on the full set', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const ids = ['a', 'b', 'c', 'd', 'e'];
        const nodes = ids.map((id) => makeNode(id, bus, clock));

        await Promise.all(nodes.map((n) => n.engine.start()));
        await converge(clock, nodes);
        for (const n of nodes) expect(viewOf(n)).toEqual(ids);

        // 'f' joins after the cluster has already settled. Its HELLO draws STATUS replies,
        // its SHARE reaches the incumbents, and periodic resync reinforces — everyone ends
        // on the full six-node set.
        const joiner = makeNode('f', bus, clock);
        const all = [...nodes, joiner];
        await joiner.engine.start();
        await converge(clock, all);

        const full = ['a', 'b', 'c', 'd', 'e', 'f'];
        for (const n of all) {
            expect(viewOf(n)).toEqual(full);
        }

        await Promise.all(all.map((n) => n.engine.stop()));
    });
});
