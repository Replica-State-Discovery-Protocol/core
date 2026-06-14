import { describe, expect, it } from 'vitest';

import type { Context } from '../../../src/domain/context.js';
import { ReducerSlot } from '../../../src/engine/internal/ReducerSlot.js';
import type { Aggregator } from '../../../src/reducer/pipeline/stages.js';
import { defineReducer, type Reducer } from '../../../src/reducer/Reducer.js';

const ctx: Context = { self: 'a' };

// Sorted-set union with a value-equality translator (changed:false at a fixpoint),
// matching the reference clusterMembers reducer — enough to exercise a slot.
const union = (self: string): Aggregator<string[], string[], Context> => ({
    aggregate: (b) => [...new Set([self, ...b.flat()])].sort(),
});
const makeReducer = (self: string): Reducer<unknown, unknown, Context> =>
    defineReducer<string[], string[], Context>('members')
        .status((p) => p.setAggregator(union(self)))
        .share((p) => p.setAggregator(union(self)))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({
            translate: (s, prev) => ({ value: s, changed: JSON.stringify(s) !== JSON.stringify(prev) }),
        }) as unknown as Reducer<unknown, unknown, Context>;

const slot = () => new ReducerSlot<Context>(makeReducer('a'));

describe('ReducerSlot', () => {
    it('starts empty: null state, version 0 payload', () => {
        const s = slot();
        expect(s.state).toBeNull();
        expect(s.toPayload()).toEqual({ value: null, version: 0 });
        expect(s.name).toBe('members');
    });

    it('runShare aggregates Σ and bumps the version only on a real change', async () => {
        const s = slot();
        s.ingestShare('b', ['b'], 1, 1000);

        expect(await s.runShare(ctx)).toBe(true);
        expect(s.state?.value).toEqual(['a', 'b']);
        expect(s.toPayload()).toEqual({ value: ['a', 'b'], version: 1 });

        // Re-running over an unchanged Σ reports no change and leaves the version put.
        expect(await s.runShare(ctx)).toBe(false);
        expect(s.toPayload().version).toBe(1);
    });

    it('version-gates Σ via ingestShare (stale/equal return false)', () => {
        const s = slot();
        expect(s.ingestShare('b', ['b'], 5, 1000)).toBe(true);
        expect(s.ingestShare('b', ['b'], 4, 1001)).toBe(false); // stale
        expect(s.ingestShare('b', ['b'], 5, 1002)).toBe(false); // equal (liveness only)
        expect(s.ingestShare('b', ['b', 'x'], 6, 1003)).toBe(true); // newer
    });

    it('runDebate converges over the UNION of the STATUS buffer and retained Σ', async () => {
        const s = slot();
        s.ingestShare('b', ['b'], 1, 1000); // retained in Σ
        s.ingestStatus('c', ['c']); // this round's STATUS only

        await s.runDebate(ctx);
        expect(s.state?.value).toEqual(['a', 'b', 'c']);

        // The DEBATE buffer is cleared after the round; a second round sees Σ only,
        // so the STATUS-only peer 'c' drops while the Σ peer 'b' is retained.
        await s.runDebate(ctx);
        expect(s.state?.value).toEqual(['a', 'b']);
    });

    it('evictDeparted clears all cached views on a real departure, no-op otherwise', () => {
        const s = slot();
        s.ingestShare('b', ['b'], 1, 1000);
        s.ingestShare('c', ['c'], 1, 1000);

        expect(s.evictDeparted('zzz')).toBe(false); // unknown peer
        expect(s.evictDeparted('b')).toBe(true); // real departure → wipe Σ
        expect(s.sweep(9999, 1)).toBe(false); // nothing left to sweep
    });

    it('sweep evicts peers unseen past the TTL', () => {
        const s = slot();
        s.ingestShare('b', ['b'], 1, 1000);
        expect(s.sweep(1400, 600)).toBe(false); // 1400 - 1000 = 400 < 600, still alive
        expect(s.sweep(1700, 600)).toBe(true); // 1700 - 1000 = 700 >= 600, evicted
    });

    it('attachShareRunner wires the single-flight steady-state task', async () => {
        const s = slot();
        let runs = 0;
        s.attachShareRunner(() => {
            runs += 1;
            return Promise.resolve();
        });
        s.trigger();
        await s.idle();
        expect(runs).toBe(1);
    });
});
