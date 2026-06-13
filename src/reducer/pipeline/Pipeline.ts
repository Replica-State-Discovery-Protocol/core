// src/reducer/pipeline/Pipeline.ts
import type { MessageType } from '../../domain/message.js';
import { GuardRejected } from '../../errors.js';
import type { Aggregate, Aggregator, Guard, Interceptor, Middleware, Normalizer, PipelineRun } from './stages.js';

export interface PipelineStages<R, M, S, Ctx> {
    middleware?: Middleware<R, S, Ctx>[];
    guards?: Guard<R, S, Ctx>[];
    interceptors?: Interceptor<M, S, Ctx>[];
    normalizer?: Normalizer<R, M, Ctx>;
    aggregator: Aggregator<M, S, Ctx>;
}

export class Pipeline<R, M, S, Ctx> {
    constructor(
        private readonly reducerName: string,
        private readonly messageType: MessageType,
        private readonly stages: PipelineStages<R, M, S, Ctx>,
    ) {}

    async run(batch: R[], ctx: Ctx, prev: S | null): Promise<S> {
        const core: PipelineRun<R, S, Ctx> = async (b, c, p) => {
            for (const guard of this.stages.guards ?? []) {
                if (!(await guard.check(b, c, p))) {
                    throw new GuardRejected(`guard rejected ${this.reducerName}/${this.messageType}`);
                }
            }
            const normalized: M[] = this.stages.normalizer
                ? await this.stages.normalizer.normalize(b, c)
                : (b as unknown as M[]);

            let aggregate: Aggregate<M, S, Ctx> = (mb, mc, mp) =>
                Promise.resolve(this.stages.aggregator.aggregate(mb, mc, mp));
            for (const interceptor of [...(this.stages.interceptors ?? [])].reverse()) {
                aggregate = interceptor.wrap(aggregate);
            }
            return aggregate(normalized, c, p);
        };

        let run = core;
        for (const mw of [...(this.stages.middleware ?? [])].reverse()) {
            run = mw.wrap(run);
        }
        return run(batch, ctx, prev);
    }
}
