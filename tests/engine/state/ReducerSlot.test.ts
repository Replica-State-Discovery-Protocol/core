import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../../src/clock/Clock.js';
import type { Context } from '../../../src/domain/context.js';
import { ReducerSlot } from '../../../src/engine/state/ReducerSlot.js';
import { TimestampIncarnation } from '../../../src/incarnation/Incarnation.js';
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

const slot = () => new ReducerSlot<Context>(makeReducer('a'), new TimestampIncarnation(new FakeClock(0)));

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

    it('ingestClose buffers only a known peer (no-op for an unknown / repeat departure)', () => {
        const s = slot();
        s.ingestShare('b', ['b'], 1, 1000);

        expect(s.ingestClose('zzz')).toBe(false); // never in Σ → ignored
        expect(s.ingestClose('b')).toBe(true); // known peer → buffered
        expect(s.ingestClose('b')).toBe(false); // already buffered this round
    });

    it('runClose runs the close pipeline over departures, removing them and evicting Σ', async () => {
        const s = slot();
        s.ingestShare('b', ['b'], 1, 1000);
        s.ingestShare('c', ['c'], 1, 1000);
        await s.runShare(ctx);
        expect(s.state?.value).toEqual(['a', 'b', 'c']);

        s.ingestClose('b');
        expect(await s.runClose(ctx)).toBe(true);
        expect(s.state?.value).toEqual(['a', 'c']); // 'b' filtered out by the close pipeline

        // 'b' was evicted from Σ, so a later steady run does not re-admit it.
        expect(await s.runShare(ctx)).toBe(false);
        expect(s.state?.value).toEqual(['a', 'c']);

        // Nothing buffered → no-op.
        expect(await s.runClose(ctx)).toBe(false);
    });

    it('sweep evicts peers unseen past the TTL', () => {
        const s = slot();
        s.ingestShare('b', ['b'], 1, 1000);
        expect(s.sweep(1400, 600)).toBe(false); // 1400 - 1000 = 400 < 600, still alive
        expect(s.sweep(1700, 600)).toBe(true); // 1700 - 1000 = 700 >= 600, evicted
    });
});

describe('ReducerSlot incarnation gating', () => {
    it('ingestShare adopts a restarted peer whose version restarted below the stored one', () => {
        const s = slot();
        s.ingestShare('b', ['b', 'stale'], 5, 1000, 100);

        expect(s.ingestShare('b', ['b'], 1, 2000, 200)).toBe(true);
    });

    it('ingestStatus drops a perspective from a strictly older lifetime', () => {
        // A STATUS redelivered from a broker queue long after the sender restarted must
        // not enter the DEBATE buffer, where nothing else would gate it.
        const s = slot();
        s.ingestShare('b', ['b'], 1, 1000, 200);

        expect(s.ingestStatus('b', ['b', 'ghost'], 100)).toBe(false);
    });

    it('ingestStatus accepts the current lifetime, an unknown peer, and an absent ι', () => {
        const s = slot();
        s.ingestShare('b', ['b'], 1, 1000, 200);

        expect(s.ingestStatus('b', ['b'], 200)).toBe(true); // same lifetime
        expect(s.ingestStatus('b', ['b'], 300)).toBe(true); // newer lifetime
        expect(s.ingestStatus('b', ['b'])).toBe(true); // legacy peer, no ι
        expect(s.ingestStatus('never-seen', ['z'], 1)).toBe(true); // nothing to compare against
    });

    it('a dropped STATUS does not reach the DEBATE round', async () => {
        const s = slot();
        s.ingestShare('b', ['b'], 1, 1000, 200);
        s.ingestStatus('b', ['b', 'ghost'], 100);

        await s.runDebate(ctx);
        expect(s.state?.value).toEqual(['a', 'b']); // 'ghost' never entered the batch
    });

    it('ingestClose ignores a departure announced by a strictly older lifetime', () => {
        // The graceful-CLOSE race: `b` stops, restarts, and its pre-restart CLOSE is
        // delivered late — it must not evict the live incarnation that replaced it.
        const s = slot();
        s.ingestShare('b', ['b'], 1, 1000, 200);

        expect(s.ingestClose('b', 100)).toBe(false);
        expect(s.ingestClose('b', 200)).toBe(true); // the current lifetime may still depart
    });
});
