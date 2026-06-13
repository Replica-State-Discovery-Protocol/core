import { describe, expect, it } from 'vitest';

import type { Context } from '../../src/domain/context.js';
import type { TranslatedState } from '../../src/domain/state.js';
import { createEngine } from '../../src/engine/Engine.js';
import type { MembersView } from '../../src/reducers/clusterMembers.js';
import { clusterMembersReducer } from '../../src/reducers/clusterMembers.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

describe('clusterMembersReducer', () => {
    it('unions peer ids with self and removes departed on close', async () => {
        const r = clusterMembersReducer('a');
        const ctx: Context = { self: 'a' };
        const status = await r.runStatus([['b'], ['c']], ctx, null);
        expect(status).toEqual(['a', 'b', 'c']);
        const closed = await r.runClose(['b'], ctx, ['a', 'b', 'c']);
        expect(closed).toEqual(['a', 'c']);
    });

    it('translator reports changed=true when membership differs, changed=false on same input', async () => {
        const r = clusterMembersReducer('a');
        const ctx: Context = { self: 'a' };
        const state = ['a', 'b'];
        const t1 = await r.translate(state, null, ctx);
        expect(t1.changed).toBe(true);
        const t2 = await r.translate(state, state, ctx);
        expect(t2.changed).toBe(false);
    });

    // Compile-time check: stateOf returns TranslatedState<MembersView> without a cast.
    it('barrel stateOf returns TranslatedState<MembersView> without cast (compile-time check)', () => {
        const r = clusterMembersReducer('node1');
        const engine = createEngine<Context>({
            identity: { address: 'node1' },
            slan: new InMemorySlan('node1', new InMemoryBus()),
            reducers: [r],
            config: { debounce: { delayMs: 10, maxWaitMs: 50 }, ttlMs: 10_000, sweepIntervalMs: 1_000 },
        });
        // The type of `result` must be TranslatedState<MembersView> | null — verified by the compiler
        // without any `as` cast. If `stateOf` were typed wrong this line would not typecheck.
        const result: TranslatedState<MembersView> | null = engine.stateOf(r);
        expect(result).toBeNull(); // engine not started, state is null
    });
});
