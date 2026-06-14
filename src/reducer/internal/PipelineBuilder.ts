// src/reducer/internal/PipelineBuilder.ts
import type { Aggregator, ExceptionFilter, Guard, Interceptor, Middleware, Normalizer } from '../pipeline/stages.js';

/**
 * Mutable accumulator for one message-type's pipeline. The fluent callback passed to
 * `ReducerBuilder.status/share/close` receives one of these and registers stages on it;
 * `ReducerBuilder.build()` then freezes it into a `Pipeline`.
 */
export class PipelineBuilder<R, M, S, Ctx> {
    middleware: Middleware<R, S, Ctx>[] = [];
    guards: Guard<R, S, Ctx>[] = [];
    interceptors: Interceptor<M, S, Ctx>[] = [];
    normalizer?: Normalizer<R, M, Ctx>;
    aggregator?: Aggregator<M, S, Ctx>;
    exceptionFilters: ExceptionFilter<Ctx>[] = [];

    addMiddleware(m: Middleware<R, S, Ctx>): this {
        this.middleware.push(m);
        return this;
    }
    addGuard(g: Guard<R, S, Ctx>): this {
        this.guards.push(g);
        return this;
    }
    addInterceptor(i: Interceptor<M, S, Ctx>): this {
        this.interceptors.push(i);
        return this;
    }
    setNormalizer<M2>(n: Normalizer<R, M2, Ctx>): PipelineBuilder<R, M2, S, Ctx> {
        const next = this as unknown as PipelineBuilder<R, M2, S, Ctx>;
        next.normalizer = n;
        return next;
    }
    setAggregator(a: Aggregator<M, S, Ctx>): this {
        this.aggregator = a;
        return this;
    }
    addExceptionFilter(f: ExceptionFilter<Ctx>): this {
        this.exceptionFilters.push(f);
        return this;
    }
}

/** The configuration callback passed to `ReducerBuilder.status/share/close`. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Configure<R, S, Ctx> = (p: PipelineBuilder<R, R, S, Ctx>) => PipelineBuilder<R, any, S, Ctx>;
