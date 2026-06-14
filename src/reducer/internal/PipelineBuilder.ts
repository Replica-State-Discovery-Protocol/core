// src/reducer/internal/PipelineBuilder.ts
import type { Aggregator, ExceptionFilter, Guard, Interceptor, Middleware, Normalizer } from '../pipeline/stages.js';

/**
 * Mutable accumulator for one message-type's pipeline. The fluent callback passed to
 * `ReducerBuilder.status/share/close` receives one of these and registers stages on it;
 * `ReducerBuilder.build()` then freezes it into a `Pipeline`. Generic over the same
 * `Raw`/`Mapped`/`State`/`Ctx` parameters as the pipeline stages.
 */
export class PipelineBuilder<Raw, Mapped, State, Ctx> {
    middleware: Middleware<Raw, State, Ctx>[] = [];
    guards: Guard<Raw, State, Ctx>[] = [];
    interceptors: Interceptor<Mapped, State, Ctx>[] = [];
    normalizer?: Normalizer<Raw, Mapped, Ctx>;
    aggregator?: Aggregator<Mapped, State, Ctx>;
    exceptionFilters: ExceptionFilter<Ctx>[] = [];

    addMiddleware(m: Middleware<Raw, State, Ctx>): this {
        this.middleware.push(m);
        return this;
    }
    addGuard(g: Guard<Raw, State, Ctx>): this {
        this.guards.push(g);
        return this;
    }
    addInterceptor(i: Interceptor<Mapped, State, Ctx>): this {
        this.interceptors.push(i);
        return this;
    }
    setNormalizer<NextMapped>(n: Normalizer<Raw, NextMapped, Ctx>): PipelineBuilder<Raw, NextMapped, State, Ctx> {
        const next = this as unknown as PipelineBuilder<Raw, NextMapped, State, Ctx>;
        next.normalizer = n;
        return next;
    }
    setAggregator(a: Aggregator<Mapped, State, Ctx>): this {
        this.aggregator = a;
        return this;
    }
    addExceptionFilter(f: ExceptionFilter<Ctx>): this {
        this.exceptionFilters.push(f);
        return this;
    }
}

/** The configuration callback passed to `ReducerBuilder.status/share/close`. */
export type Configure<Raw, State, Ctx> = (
    p: PipelineBuilder<Raw, Raw, State, Ctx>,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
) => PipelineBuilder<Raw, any, State, Ctx>;
