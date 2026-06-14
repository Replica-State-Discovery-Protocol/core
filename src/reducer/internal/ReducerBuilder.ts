// src/reducer/internal/ReducerBuilder.ts
import type { Address, ReducerName } from '../../domain/address.js';
import type { Context } from '../../domain/context.js';
import { MessageType } from '../../domain/message.js';
import { ConfigError } from '../../errors.js';
import { Pipeline } from '../pipeline/Pipeline.js';
import { Reducer, type Translator } from '../Reducer.js';
import { type Configure, PipelineBuilder } from './PipelineBuilder.js';

/**
 * Fluent builder behind `defineReducer`. Collects a per-message-type pipeline plus a
 * translator, then validates and assembles them into an immutable {@link Reducer}.
 */
export class ReducerBuilder<S, V, Ctx extends Context> {
    private statusB?: PipelineBuilder<V, unknown, S, Ctx>;
    private shareB?: PipelineBuilder<V, unknown, S, Ctx>;
    private closeB?: PipelineBuilder<Address, unknown, S, Ctx>;
    private translator?: Translator<S, V, Ctx>;

    constructor(private readonly name: ReducerName) {}

    status(fn: Configure<V, S, Ctx>): this {
        this.statusB = fn(new PipelineBuilder<V, V, S, Ctx>());
        return this;
    }
    share(fn: Configure<V, S, Ctx>): this {
        this.shareB = fn(new PipelineBuilder<V, V, S, Ctx>());
        return this;
    }
    close(fn: Configure<Address, S, Ctx>): this {
        this.closeB = fn(new PipelineBuilder<Address, Address, S, Ctx>());
        return this;
    }
    setTranslator(t: Translator<S, V, Ctx>): Reducer<S, V, Ctx> {
        this.translator = t;
        return this.build();
    }

    /** Validate and assemble. Throws ConfigError if anything required is missing. */
    build(): Reducer<S, V, Ctx> {
        const need = <T>(v: T | undefined, what: string): T => {
            if (v === undefined) throw new ConfigError(`reducer "${this.name}" is missing ${what}`);
            return v;
        };
        const status = need(this.statusB, 'a STATUS pipeline');
        const share = need(this.shareB, 'a SHARE pipeline');
        const close = need(this.closeB, 'a CLOSE pipeline');
        const translator = need(this.translator, 'a translator');
        for (const [b, label] of [
            [status, MessageType.Status],
            [share, MessageType.Share],
            [close, MessageType.Close],
        ] as const) {
            if (!b.aggregator) throw new ConfigError(`reducer "${this.name}" ${label} pipeline has no aggregator`);
        }
        return new Reducer<S, V, Ctx>(
            this.name,
            new Pipeline(this.name, MessageType.Status, { ...status, aggregator: status.aggregator! }),
            new Pipeline(this.name, MessageType.Share, { ...share, aggregator: share.aggregator! }),
            new Pipeline(this.name, MessageType.Close, { ...close, aggregator: close.aggregator! }),
            translator,
            {
                status: status.exceptionFilters,
                share: share.exceptionFilters,
                close: close.exceptionFilters,
            },
        );
    }
}
