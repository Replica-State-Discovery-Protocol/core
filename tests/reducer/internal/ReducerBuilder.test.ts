import { describe, expect, it } from 'vitest';

import type { Context } from '../../../src/domain/context.js';
import { ConfigError } from '../../../src/errors.js';
import { ReducerBuilder } from '../../../src/reducer/internal/ReducerBuilder.js';
import type { Aggregator } from '../../../src/reducer/pipeline/stages.js';

type V = number[];
const ctx: Context = { self: 's' };
const agg: Aggregator<V, V, Context> = { aggregate: (b) => b.flat() };

// All three pipelines configured, but no translator yet.
const withPipelines = () =>
    new ReducerBuilder<V, V, Context>('nums')
        .status((p) => p.setAggregator(agg))
        .share((p) => p.setAggregator(agg))
        .close((p) => p.setAggregator({ aggregate: (_b, _c, prev) => prev ?? [] }));

describe('ReducerBuilder', () => {
    it('assembles a working Reducer once pipelines + translator are set', async () => {
        const r = withPipelines().setTranslator({ translate: (s) => ({ value: s, changed: true }) });

        expect(r.name).toBe('nums');
        expect(await r.runStatus([[1], [2, 3]], ctx, null)).toEqual([1, 2, 3]);
        expect(await r.runShare([[4]], ctx, null)).toEqual([4]);
        expect(await r.runClose([], ctx, [9])).toEqual([9]);
        await expect(r.translate([1], null, ctx)).resolves.toEqual({ value: [1], changed: true });
    });

    it('throws ConfigError naming a missing pipeline', () => {
        expect(() => new ReducerBuilder<V, V, Context>('x').build()).toThrow(ConfigError);
        expect(() => new ReducerBuilder<V, V, Context>('x').build()).toThrow(/missing a STATUS pipeline/);
    });

    it('throws ConfigError when the translator is missing', () => {
        expect(() => withPipelines().build()).toThrow(/missing a translator/);
    });

    it('throws ConfigError when a configured pipeline has no aggregator', () => {
        const b = new ReducerBuilder<V, V, Context>('y')
            .status((p) => p) // configured but no aggregator
            .share((p) => p.setAggregator(agg))
            .close((p) => p.setAggregator({ aggregate: (_b, _c, prev) => prev ?? [] }));
        // setTranslator triggers build(), which reaches the per-pipeline aggregator check.
        expect(() => b.setTranslator({ translate: (s) => ({ value: s, changed: false }) })).toThrow(
            /STATUS pipeline has no aggregator/,
        );
    });
});
