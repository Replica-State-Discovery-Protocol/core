// tests/reducer/Reducer.test.ts
import { describe, expect, it } from 'vitest';

import type { Context } from '../../src/domain/context.js';
import { ConfigError } from '../../src/errors.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';
import { defineReducer } from '../../src/reducer/Reducer.js';

type V = number[];
const sumInto: Aggregator<V, V, Context> = { aggregate: (batch) => batch.flat() };

describe('defineReducer', () => {
    it('builds a reducer with three pipelines and a translator', async () => {
        const r = defineReducer<V, V, Context>('nums')
            .status((p) => p.setAggregator(sumInto))
            .share((p) => p.setAggregator(sumInto))
            .close((p) => p.setAggregator({ aggregate: (_b, _c, prev) => prev ?? [] }))
            .setTranslator({ translate: (state) => ({ value: state, changed: true }) });

        expect(r.name).toBe('nums');
        const out = await r.runShare([[1], [2, 3]], { self: 's' }, null);
        expect(out).toEqual([1, 2, 3]);
        await expect(r.translate([1, 2, 3], null, { self: 's' })).resolves.toEqual({ value: [1, 2, 3], changed: true });
    });

    it('verify() throws ConfigError when a pipeline or translator is missing', () => {
        const incomplete = defineReducer<V, V, Context>('bad').status((p) => p.setAggregator(sumInto));
        expect(() => incomplete.build()).toThrow(ConfigError);
    });
});
