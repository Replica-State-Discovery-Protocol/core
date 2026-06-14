// src/reducer/pipeline/stages.ts
//
// Stage interfaces are parameterised by:
//   Raw    — the raw element type entering the pipeline (a peer's View, or an Address for CLOSE)
//   Mapped — the element type after the normalizer (defaults to Raw when none is set)
//   State  — the reducer's internal converged state
//   Ctx    — the per-run context
export type PipelineRun<Raw, State, Ctx> = (batch: Raw[], ctx: Ctx, prev: State | null) => Promise<State>;
export type Aggregate<Mapped, State, Ctx> = (batch: Mapped[], ctx: Ctx, prev: State | null) => Promise<State>;

export interface Middleware<Raw, State, Ctx> {
    wrap(next: PipelineRun<Raw, State, Ctx>): PipelineRun<Raw, State, Ctx>;
}

export interface Guard<Raw, State, Ctx> {
    check(batch: Raw[], ctx: Ctx, prev: State | null): boolean | Promise<boolean>;
}

export interface Interceptor<Mapped, State, Ctx> {
    wrap(next: Aggregate<Mapped, State, Ctx>): Aggregate<Mapped, State, Ctx>;
}

export interface Normalizer<Raw, Mapped, Ctx> {
    normalize(batch: Raw[], ctx: Ctx): Mapped[] | Promise<Mapped[]>;
}

export interface Aggregator<Mapped, State, Ctx> {
    aggregate(batch: Mapped[], ctx: Ctx, prev: State | null): State | Promise<State>;
}

export interface ExceptionFilter<Ctx> {
    handle(err: unknown, ctx: Ctx): void | Promise<void>;
}
