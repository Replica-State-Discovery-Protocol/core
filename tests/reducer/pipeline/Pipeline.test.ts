// tests/reducer/pipeline/Pipeline.test.ts
import { describe, expect, it } from 'vitest';

import type { Context } from '../../../src/domain/context.js';
import { MessageType } from '../../../src/domain/message.js';
import { GuardRejected } from '../../../src/errors.js';
import { Pipeline } from '../../../src/reducer/pipeline/Pipeline.js';
import type { Aggregator, Guard, Interceptor, Middleware, Normalizer } from '../../../src/reducer/pipeline/stages.js';

const ctx: Context = { self: 'self' };

describe('Pipeline', () => {
    it('runs normalizer then aggregator', async () => {
        const normalizer: Normalizer<number, number, Context> = { normalize: (b) => b.map((n) => n * 10) };
        const aggregator: Aggregator<number, number, Context> = { aggregate: (b) => b.reduce((a, n) => a + n, 0) };
        const p = new Pipeline<number, number, number, Context>('r', MessageType.Share, { normalizer, aggregator });
        expect(await p.run([1, 2, 3], ctx, null)).toBe(60);
    });

    it('rejects when a guard returns false', async () => {
        const aggregator: Aggregator<number, number, Context> = { aggregate: () => 1 };
        const guard: Guard<number, number, Context> = { check: (b) => b.length > 0 };
        const p = new Pipeline<number, number, number, Context>('r', MessageType.Share, {
            aggregator,
            guards: [guard],
        });
        await expect(p.run([], ctx, null)).rejects.toBeInstanceOf(GuardRejected);
    });

    it('wraps aggregation with interceptors and the whole run with middleware (outermost first)', async () => {
        const order: string[] = [];
        const aggregator: Aggregator<number, number, Context> = {
            aggregate: () => {
                order.push('agg');
                return 1;
            },
        };
        const interceptor: Interceptor<number, number, Context> = {
            wrap: (next) => async (b, c, prev) => {
                order.push('int:before');
                const r = await next(b, c, prev);
                order.push('int:after');
                return r;
            },
        };
        const middleware: Middleware<number, number, Context> = {
            wrap: (next) => async (b, c, prev) => {
                order.push('mw:before');
                const r = await next(b, c, prev);
                order.push('mw:after');
                return r;
            },
        };
        const p = new Pipeline<number, number, number, Context>('r', MessageType.Share, {
            aggregator,
            interceptors: [interceptor],
            middleware: [middleware],
        });
        await p.run([1], ctx, null);
        expect(order).toEqual(['mw:before', 'int:before', 'agg', 'int:after', 'mw:after']);
    });
});
