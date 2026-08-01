import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../../src/clock/Clock.js';
import type { Context } from '../../../src/domain/context.js';
import { ReducerSlot } from '../../../src/engine/state/ReducerSlot.js';
import { SlotRegistry } from '../../../src/engine/state/SlotRegistry.js';
import { TimestampIncarnation } from '../../../src/incarnation/Incarnation.js';
import type { Aggregator } from '../../../src/reducer/pipeline/stages.js';
import { defineReducer, type Reducer } from '../../../src/reducer/Reducer.js';

const ctx: Context = { self: 'a' };
const inc = (): TimestampIncarnation => new TimestampIncarnation(new FakeClock(0));

const union = (self: string): Aggregator<string[], string[], Context> => ({
    aggregate: (b) => [...new Set([self, ...b.flat()])].sort(),
});
const makeReducer = (name: string): Reducer<unknown, unknown, Context> =>
    defineReducer<string[], string[], Context>(name)
        .status((p) => p.setAggregator(union('a')))
        .share((p) => p.setAggregator(union('a')))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({
            translate: (s, prev) => ({ value: s, changed: JSON.stringify(s) !== JSON.stringify(prev) }),
        }) as unknown as Reducer<unknown, unknown, Context>;

describe('SlotRegistry', () => {
    it('adds, looks up by name, and iterates slots', () => {
        const reg = new SlotRegistry<Context>();
        const m = new ReducerSlot<Context>(makeReducer('members'), inc());
        const p = new ReducerSlot<Context>(makeReducer('peers'), inc());
        reg.add(m);
        reg.add(p);

        expect(reg.get('members')).toBe(m);
        expect(reg.get('missing')).toBeUndefined();
        expect([...reg.values()]).toEqual([m, p]);
    });

    it('composite() emits one versioned payload per reducer', async () => {
        const reg = new SlotRegistry<Context>();
        const m = new ReducerSlot<Context>(makeReducer('members'), inc());
        reg.add(m);
        reg.add(new ReducerSlot<Context>(makeReducer('peers'), inc()));

        m.ingestShare('b', ['b'], 1, 1000);
        await m.runShare(ctx);

        expect(reg.composite()).toEqual({
            members: { value: ['a', 'b'], version: 1 },
            peers: { value: null, version: 0 },
        });
    });

    it('exposes state by reducer handle and by name, and via the snapshot view', async () => {
        const reg = new SlotRegistry<Context>();
        const reducer = makeReducer('members');
        const m = new ReducerSlot<Context>(reducer, inc());
        reg.add(m);
        await m.runShare(ctx); // self-only → ['a']

        expect(reg.stateOf(reducer)?.value).toEqual(['a']);
        expect(reg.stateByName('members')?.value).toEqual(['a']);
        expect(reg.stateByName('nope')).toBeNull();
        expect(reg.snapshotView().get(reducer)?.value).toEqual(['a']);
    });

    it('stateOf returns null for a reducer with no slot', () => {
        const reg = new SlotRegistry<Context>();
        expect(reg.stateOf(makeReducer('absent'))).toBeNull();
    });

    it('sweepExpired reports evictions per reducer, omitting slots that lost nothing', () => {
        // Σ is per (reducer, peer): the same sweep can evict from one reducer and not another,
        // so the report must name the reducer rather than collapse to a single set.
        const reg = new SlotRegistry<Context>();
        const members = new ReducerSlot<Context>(makeReducer('members'), inc());
        const peers = new ReducerSlot<Context>(makeReducer('peers'), inc());
        reg.add(members);
        reg.add(peers);

        members.ingestShare('b', ['b'], 1, 1000);
        peers.ingestShare('b', ['b'], 1, 1000);
        peers.ingestShare('c', ['c'], 1, 5000); // still fresh at the sweep below

        expect(reg.sweepExpired(1400, 600)).toEqual([]);
        expect(reg.sweepExpired(1700, 600)).toEqual([
            { reducer: 'members', addrs: ['b'] },
            { reducer: 'peers', addrs: ['b'] },
        ]);
    });

    it('sigmaOf exposes a reducer’s Σ, and an empty list for an unknown reducer', () => {
        const reg = new SlotRegistry<Context>();
        const reducer = makeReducer('members');
        const m = new ReducerSlot<Context>(reducer, inc());
        reg.add(m);
        m.ingestShare('b', ['b'], 2, 1000, 42);

        expect(reg.sigmaOf(reducer)).toEqual([{ addr: 'b', version: 2, inc: 42, lastSeenAt: 1000 }]);
        expect(reg.sigmaOf(makeReducer('absent'))).toEqual([]);
    });
});
