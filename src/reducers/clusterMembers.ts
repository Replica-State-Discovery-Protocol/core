import type { Context } from '../domain/context.js';
import type { Aggregator } from '../reducer/pipeline/stages.js';
import { defineReducer } from '../reducer/Reducer.js';

export type MembersView = string[];

const union = (self: string): Aggregator<MembersView, MembersView, Context> => ({
    aggregate: (batch) => [...new Set([self, ...batch.flat()])].sort(),
});

/** Reference reducer: maintains the sorted set of cluster member ids. */
export function clusterMembersReducer(self: string) {
    return defineReducer<MembersView, MembersView, Context>('cluster-members')
        .status((p) => p.setAggregator(union(self)))
        .share((p) => p.setAggregator(union(self)))
        .close((p) =>
            p.setAggregator({ aggregate: (departed, _c, prev) => (prev ?? []).filter((id) => !departed.includes(id)) }),
        )
        .setTranslator({
            translate: (state, prev) => ({ value: state, changed: JSON.stringify(state) !== JSON.stringify(prev) }),
        });
}
