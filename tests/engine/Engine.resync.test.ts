import { describe, expect, it, vi } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Context } from '../../src/domain/context.js';
import { MessageType } from '../../src/domain/message.js';
import { createEngine } from '../../src/engine/Engine.js';
import type { Reducer } from '../../src/reducer/Reducer.js';
import { clusterMembersReducer } from '../../src/reducers/clusterMembers.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

// clusterMembersReducer uses a VALUE-EQUALITY translator (changed:false at a
// fixpoint), so once the cluster converges the engine must stop broadcasting.
type V = string[];
const reducer = (self: string): Reducer<unknown, V, Context> =>
    clusterMembersReducer(self) as unknown as Reducer<unknown, V, Context>;

const cfg = { debounce: { delayMs: 10, maxWaitMs: 50 }, ttlMs: 10_000, sweepIntervalMs: 1_000 };

const settleAll = async (clock: FakeClock, engines: { settle(): Promise<void> }[]) => {
    for (let i = 0; i < 10; i++) {
        clock.advance(60);
        await Promise.all(engines.map((e) => e.settle()));
        await Promise.resolve();
    }
};

describe('Engine fixpoint silence and SHARE dedup', () => {
    it('converges to full membership then the bus goes quiet', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const mk = (id: string) => {
            const r = reducer(id);
            const slan = new InMemorySlan(id, bus);
            const broadcastSpy = vi.spyOn(slan, 'broadcast');
            const engine = createEngine<Context>({
                identity: { address: id },
                slan,
                reducers: [r],
                config: cfg,
                clock,
            });
            return { id, r, engine, broadcastSpy };
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

        // Record broadcast counts after a final settle round, then settle once
        // more WITHOUT crossing ttlMs and assert no node broadcast again.
        const before = nodes.map((n) => n.broadcastSpy.mock.calls.length);
        await settleAll(
            clock,
            nodes.map((n) => n.engine),
        );
        const after = nodes.map((n) => n.broadcastSpy.mock.calls.length);
        expect(after).toEqual(before);

        await Promise.all(nodes.map((n) => n.engine.stop()));
    });

    it('ignores a duplicate SHARE (same from/version/value) on the second delivery', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const r = reducer('a');
        const slanA = new InMemorySlan('a', bus);
        const engine = createEngine<Context>({
            identity: { address: 'a' },
            slan: slanA,
            reducers: [r],
            config: cfg,
            clock,
        });
        const peer = new InMemorySlan('peer', bus);
        await peer.init();
        await engine.start();
        clock.advance(60);
        await engine.settle();

        const share = {
            type: MessageType.Share,
            from: 'peer',
            payloads: { 'cluster-members': { value: ['peer'], version: 5 } },
        } as const;

        await peer.sendTo('a', share);
        await settleAll(clock, [engine]);
        expect(engine.stateOf(r)?.value).toEqual(['a', 'peer']);

        const spy = vi.spyOn(slanA, 'broadcast');
        // Re-send the identical SHARE: the version gate must drop it, so the
        // membership is unchanged and no new broadcast is triggered.
        await peer.sendTo('a', share);
        await settleAll(clock, [engine]);
        expect(engine.stateOf(r)?.value).toEqual(['a', 'peer']);
        expect(spy.mock.calls.length).toBe(0);

        await engine.stop();
    });

    it('ignores an out-of-order SHARE (lower version than stored)', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const r = reducer('a');
        const slanA = new InMemorySlan('a', bus);
        const engine = createEngine<Context>({
            identity: { address: 'a' },
            slan: slanA,
            reducers: [r],
            config: cfg,
            clock,
        });
        const peer = new InMemorySlan('peer', bus);
        await peer.init();
        await engine.start();
        clock.advance(60);
        await engine.settle();

        // Establish a high stored version for 'peer' carrying ['peer','x'].
        await peer.sendTo('a', {
            type: MessageType.Share,
            from: 'peer',
            payloads: { 'cluster-members': { value: ['peer', 'x'], version: 10 } },
        });
        await settleAll(clock, [engine]);
        expect(engine.stateOf(r)?.value).toEqual(['a', 'peer', 'x']);

        // A stale, lower-version SHARE that would drop 'x' must be ignored.
        await peer.sendTo('a', {
            type: MessageType.Share,
            from: 'peer',
            payloads: { 'cluster-members': { value: ['peer'], version: 3 } },
        });
        await settleAll(clock, [engine]);
        expect(engine.stateOf(r)?.value).toEqual(['a', 'peer', 'x']);

        await engine.stop();
    });
});
