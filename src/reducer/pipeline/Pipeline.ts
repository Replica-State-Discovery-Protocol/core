// src/reducer/pipeline/Pipeline.ts
import type { MessageType } from '../../domain/message.js';
import { GuardRejected } from '../../errors.js';
import type { Aggregate, Aggregator, Guard, Interceptor, Middleware, Normalizer, PipelineRun } from './stages.js';

export interface PipelineStages<Raw, Mapped, State, Ctx> {
    middleware?: Middleware<Raw, State, Ctx>[];
    guards?: Guard<Raw, State, Ctx>[];
    interceptors?: Interceptor<Mapped, State, Ctx>[];
    normalizer?: Normalizer<Raw, Mapped, Ctx>;
    aggregator: Aggregator<Mapped, State, Ctx>;
}

export class Pipeline<Raw, Mapped, State, Ctx> {
    constructor(
        private readonly reducerName: string,
        private readonly messageType: MessageType,
        private readonly stages: PipelineStages<Raw, Mapped, State, Ctx>,
    ) {}

    /** Outermost entry: wrap the core run with middleware (outside-in), then invoke it. */
    run(batch: Raw[], ctx: Ctx, prev: State | null): Promise<State> {
        let run: PipelineRun<Raw, State, Ctx> = this.runCore.bind(this);

        for (const mw of [...(this.stages.middleware ?? [])].reverse()) {
            run = mw.wrap(run);
        }

        return run(batch, ctx, prev);
    }

    /** The inner pipeline (inside all middleware): guards → normalize → interceptors → aggregate. */
    private async runCore(batch: Raw[], ctx: Ctx, prev: State | null): Promise<State> {
        for (const guard of this.stages.guards ?? []) {
            if (!(await guard.check(batch, ctx, prev))) {
                throw new GuardRejected(`guard rejected ${this.reducerName}/${this.messageType}`);
            }
        }

        const normalized: Mapped[] = this.stages.normalizer
            ? await this.stages.normalizer.normalize(batch, ctx)
            : (batch as unknown as Mapped[]);

        let aggregate: Aggregate<Mapped, State, Ctx> = this.aggregate.bind(this);
        for (const interceptor of [...(this.stages.interceptors ?? [])].reverse()) {
            aggregate = interceptor.wrap(aggregate);
        }
        return aggregate(normalized, ctx, prev);
    }

    /** The base aggregate step, before interceptors wrap it. */
    private aggregate(batch: Mapped[], ctx: Ctx, prev: State | null): Promise<State> {
        return Promise.resolve(this.stages.aggregator.aggregate(batch, ctx, prev));
    }
}
