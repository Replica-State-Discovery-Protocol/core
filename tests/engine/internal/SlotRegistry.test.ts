import { describe, expect, it } from 'vitest';

import type { Context } from '../../../src/domain/context.js';
import { ReducerSlot } from '../../../src/engine/internal/ReducerSlot.js';
import { SlotRegistry } from '../../../src/engine/internal/SlotRegistry.js';
import type { Aggregator } from '../../../src/reducer/pipeline/stages.js';
import { defineReducer, type Reducer } from '../../../src/reducer/Reducer.js';

const ctx: Context = { self: 'a' };

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
        const m = new ReducerSlot<Context>(makeReducer('members'));
        const p = new ReducerSlot<Context>(makeReducer('peers'));
        reg.add(m);
        reg.add(p);

        expect(reg.get('members')).toBe(m);
        expect(reg.get('missing')).toBeUndefined();
        expect([...reg.values()]).toEqual([m, p]);
    });

    it('composite() emits one versioned payload per reducer', async () => {
        const reg = new SlotRegistry<Context>();
        const m = new ReducerSlot<Context>(makeReducer('members'));
        reg.add(m);
        reg.add(new ReducerSlot<Context>(makeReducer('peers')));

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
        const m = new ReducerSlot<Context>(reducer);
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
});
