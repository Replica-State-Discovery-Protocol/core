// src/reducer/Reducer.ts
import type { Address, ReducerName } from '../domain/address.js';
import type { Context } from '../domain/context.js';
import { MessageType } from '../domain/message.js';
import type { TranslatedState } from '../domain/state.js';
import { ConfigError } from '../errors.js';
import { Pipeline } from './pipeline/Pipeline.js';
import type { Aggregator, ExceptionFilter, Guard, Interceptor, Middleware, Normalizer } from './pipeline/stages.js';

export interface Translator<S, V, Ctx> {
    translate(state: S | null, prev: S | null, ctx: Ctx): TranslatedState<V> | Promise<TranslatedState<V>>;
}

class PipelineBuilder<R, M, S, Ctx> {
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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Configure<R, S, Ctx> = (p: PipelineBuilder<R, R, S, Ctx>) => PipelineBuilder<R, any, S, Ctx>;

export class Reducer<S, V, Ctx extends Context> {
    constructor(
        readonly name: ReducerName,
        private readonly statusPipe: Pipeline<V, unknown, S, Ctx>,
        private readonly sharePipe: Pipeline<V, unknown, S, Ctx>,
        private readonly closePipe: Pipeline<Address, unknown, S, Ctx>,
        private readonly translator: Translator<S, V, Ctx>,
        readonly exceptionFilters: {
            status: ExceptionFilter<Ctx>[];
            share: ExceptionFilter<Ctx>[];
            close: ExceptionFilter<Ctx>[];
        },
    ) {}

    runStatus(batch: V[], ctx: Ctx, prev: S | null): Promise<S> {
        return this.statusPipe.run(batch, ctx, prev);
    }
    runShare(batch: V[], ctx: Ctx, prev: S | null): Promise<S> {
        return this.sharePipe.run(batch, ctx, prev);
    }
    runClose(batch: Address[], ctx: Ctx, prev: S | null): Promise<S> {
        return this.closePipe.run(batch, ctx, prev);
    }
    translate(state: S | null, prev: S | null, ctx: Ctx): Promise<TranslatedState<V>> {
        return Promise.resolve(this.translator.translate(state, prev, ctx));
    }
}

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
            [status, 'STATUS'],
            [share, 'SHARE'],
            [close, 'CLOSE'],
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

export function defineReducer<S, V, Ctx extends Context = Context>(name: ReducerName): ReducerBuilder<S, V, Ctx> {
    return new ReducerBuilder<S, V, Ctx>(name);
}
