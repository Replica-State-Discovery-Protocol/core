import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Context } from '../../src/domain/context.js';
import { MessageType } from '../../src/domain/message.js';
import { createEngine } from '../../src/engine/Engine.js';
import type { Reducer } from '../../src/reducer/Reducer.js';
import { clusterMembersReducer } from '../../src/reducers/clusterMembers.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

// clusterMembersReducer uses a VALUE-EQUALITY translator (changed:false at a
// fixpoint), so once the cluster converges no node bumps its version. But RSDP
// is eventually-consistent and MUST NOT go silent: nodes periodically re-broadcast
// HELLO (resync), redoing the handshake so peers reply with STATUS and each node
// re-derives its view and re-broadcasts SHARE. That keeps peer liveness fresh so TTL
// only evicts genuinely-departed nodes — never healthy members.
type V = string[];
const reducer = (self: string): Reducer<unknown, V, Context> =>
    clusterMembersReducer(self) as unknown as Reducer<unknown, V, Context>;

const cfg = {
    debounce: { delayMs: 10, maxWaitMs: 50 },
    ttlMs: 10_000,
    sweepIntervalMs: 1_000,
    resyncIntervalMs: 1_000,
};

const settleAll = async (clock: FakeClock, engines: { settle(): Promise<void> }[]) => {
    for (let i = 0; i < 10; i++) {
        clock.advance(60);
        await Promise.all(engines.map((e) => e.settle()));
        await Promise.resolve();
    }
};

describe('Engine periodic resync and SHARE dedup', () => {
    it('converges to full membership, stays stable, and resync keeps members alive past TTL', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const mk = (id: string) => {
            const r = reducer(id);
            const slan = new InMemorySlan(id, bus);
            const engine = createEngine<Context>({
                identity: { address: id },
                slan,
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

        // The converged VALUE stays stable across further settles.
        await settleAll(
            clock,
            nodes.map((n) => n.engine),
        );
        for (const n of nodes) {
            expect(n.engine.stateOf(n.r)?.value).toEqual(['a', 'b', 'c']);
        }

        // Crucially: advance the clock WELL PAST ttlMs (10_000), across many
        // resync intervals (1_000). Periodic resync keeps peers' liveness fresh,
        // so TTL must NOT evict any healthy member — every node still has the
        // full membership.
        for (let i = 0; i < 40; i++) {
            clock.advance(1_000); // 40_000 ms total > 4 * ttlMs, dozens of resync intervals
            await Promise.all(nodes.map((n) => n.engine.settle()));
            await Promise.resolve();
        }
        for (const n of nodes) {
            expect(n.engine.stateOf(n.r)?.value).toEqual(['a', 'b', 'c']);
        }

        await Promise.all(nodes.map((n) => n.engine.stop()));
    });

    it('retains a Σ peer across resync rounds within TTL, then evicts it once TTL lapses', async () => {
        // Regression: a resync re-runs DEBATE, but DEBATE must converge over the UNION
        // of this round's STATUS replies AND retained Σ — not the transient STATUS
        // buffer alone. A peer whose SHARE is in Σ but which does not reply to our
        // resync HELLOs must survive every resync round until its TTL genuinely lapses,
        // never be dropped-and-re-added each round.
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const r = reducer('a');
        const engine = createEngine<Context>({
            identity: { address: 'a' },
            slan: new InMemorySlan('a', bus),
            reducers: [r],
            config: {
                debounce: { delayMs: 10, maxWaitMs: 50 },
                ttlMs: 1_000,
                sweepIntervalMs: 100,
                resyncIntervalMs: 100,
            },
            clock,
        });
        const peer = new InMemorySlan('p', bus);
        await peer.init();
        await engine.start();
        clock.advance(60);
        await engine.settle();

        // p contributes one SHARE then goes silent (never replies to our resync HELLOs).
        await peer.sendTo('a', {
            type: MessageType.Share,
            from: 'p',
            payloads: { 'cluster-members': { value: ['p'], version: 1 } },
        });
        clock.advance(60);
        await engine.settle();
        expect(engine.stateOf(r)?.value).toEqual(['a', 'p']);

        // Cross several resync intervals (100ms) but stay under TTL (1_000ms): each
        // resync re-debates from an empty STATUS buffer, yet Σ retains 'p'.
        for (let i = 0; i < 7; i++) {
            clock.advance(100);
            await engine.settle();
            await Promise.resolve();
            expect(engine.stateOf(r)?.value).toEqual(['a', 'p']);
        }

        // Now lapse TTL: 'p' has not been seen since its single SHARE → swept.
        clock.advance(1_500);
        await engine.settle();
        expect(engine.stateOf(r)?.value).toEqual(['a']);

        await engine.stop();
    });

    it('ignores a duplicate SHARE (same from/version/value): no state change, liveness refresh only', async () => {
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

        // Re-send the identical SHARE: the version gate refreshes liveness only and
        // reports no STATE change, so the membership is unchanged.
        await peer.sendTo('a', share);
        await settleAll(clock, [engine]);
        expect(engine.stateOf(r)?.value).toEqual(['a', 'peer']);

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
