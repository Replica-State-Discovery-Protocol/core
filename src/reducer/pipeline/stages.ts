// src/reducer/pipeline/stages.ts
export type PipelineRun<R, S, Ctx> = (batch: R[], ctx: Ctx, prev: S | null) => Promise<S>;
export type Aggregate<M, S, Ctx> = (batch: M[], ctx: Ctx, prev: S | null) => Promise<S>;

export interface Middleware<R, S, Ctx> {
    wrap(next: PipelineRun<R, S, Ctx>): PipelineRun<R, S, Ctx>;
}

export interface Guard<R, S, Ctx> {
    check(batch: R[], ctx: Ctx, prev: S | null): boolean | Promise<boolean>;
}

export interface Interceptor<M, S, Ctx> {
    wrap(next: Aggregate<M, S, Ctx>): Aggregate<M, S, Ctx>;
}

export interface Normalizer<R, M, Ctx> {
    normalize(batch: R[], ctx: Ctx): M[] | Promise<M[]>;
}

export interface Aggregator<M, S, Ctx> {
    aggregate(batch: M[], ctx: Ctx, prev: S | null): S | Promise<S>;
}

export interface ExceptionFilter<Ctx> {
    handle(err: unknown, ctx: Ctx): void | Promise<void>;
}
