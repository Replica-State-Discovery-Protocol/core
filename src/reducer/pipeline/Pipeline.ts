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

    async run(batch: Raw[], ctx: Ctx, prev: State | null): Promise<State> {
        const core: PipelineRun<Raw, State, Ctx> = async (b, c, p) => {
            for (const guard of this.stages.guards ?? []) {
                if (!(await guard.check(b, c, p))) {
                    throw new GuardRejected(`guard rejected ${this.reducerName}/${this.messageType}`);
                }
            }
            const normalized: Mapped[] = this.stages.normalizer
                ? await this.stages.normalizer.normalize(b, c)
                : (b as unknown as Mapped[]);

            let aggregate: Aggregate<Mapped, State, Ctx> = (mb, mc, mp) =>
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
