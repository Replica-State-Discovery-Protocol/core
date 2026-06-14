import { describe, expect, it } from 'vitest';

import type { Context } from '../../../src/domain/context.js';
import { PipelineBuilder } from '../../../src/reducer/internal/PipelineBuilder.js';
import type {
    Aggregator,
    ExceptionFilter,
    Guard,
    Interceptor,
    Middleware,
    Normalizer,
} from '../../../src/reducer/pipeline/stages.js';

type R = number;
type S = number[];

describe('PipelineBuilder', () => {
    it('starts with empty stage lists and no normalizer/aggregator', () => {
        const b = new PipelineBuilder<R, R, S, Context>();
        expect(b.middleware).toEqual([]);
        expect(b.guards).toEqual([]);
        expect(b.interceptors).toEqual([]);
        expect(b.exceptionFilters).toEqual([]);
        expect(b.normalizer).toBeUndefined();
        expect(b.aggregator).toBeUndefined();
    });

    it('accumulates stages and returns `this` for chaining', () => {
        const b = new PipelineBuilder<R, R, S, Context>();
        const mw: Middleware<R, S, Context> = { wrap: (n) => n };
        const g: Guard<R, S, Context> = { check: () => true };
        const ic: Interceptor<R, S, Context> = { wrap: (n) => n };
        const ef: ExceptionFilter<Context> = { handle: () => undefined };
        const agg: Aggregator<R, S, Context> = { aggregate: (batch) => batch };

        expect(b.addMiddleware(mw)).toBe(b);
        expect(b.addGuard(g)).toBe(b);
        expect(b.addInterceptor(ic)).toBe(b);
        expect(b.addExceptionFilter(ef)).toBe(b);
        expect(b.setAggregator(agg)).toBe(b);

        expect(b.middleware).toEqual([mw]);
        expect(b.guards).toEqual([g]);
        expect(b.interceptors).toEqual([ic]);
        expect(b.exceptionFilters).toEqual([ef]);
        expect(b.aggregator).toBe(agg);
    });

    it('setNormalizer stores the normalizer and returns the same (retyped) instance', () => {
        const b = new PipelineBuilder<R, R, S, Context>();
        const norm: Normalizer<R, string, Context> = { normalize: (batch) => batch.map(String) };

        const next = b.setNormalizer(norm);
        expect(Object.is(next, b)).toBe(true);
        expect(next.normalizer).toBe(norm);
    });
});
