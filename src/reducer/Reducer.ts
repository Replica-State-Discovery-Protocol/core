// src/reducer/Reducer.ts
import type { Address, ReducerName } from '../domain/address.js';
import type { Context } from '../domain/context.js';
import type { TranslatedState } from '../domain/state.js';
import { ReducerBuilder } from './internal/ReducerBuilder.js';
import type { Pipeline } from './pipeline/Pipeline.js';
import type { ExceptionFilter } from './pipeline/stages.js';

/** Derives the public view `V` from the internal converged state `S`. */
export interface Translator<S, V, Ctx> {
    translate(state: S | null, prev: S | null, ctx: Ctx): TranslatedState<V> | Promise<TranslatedState<V>>;
}

/**
 * An assembled, immutable reducer: three message-type pipelines (STATUS / SHARE / CLOSE),
 * a translator, and the per-stage exception filters. Built via {@link defineReducer}; the
 * engine drives it through `runStatus`/`runShare`/`runClose` + `translate`.
 */
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

/** Entry point: start a fluent {@link ReducerBuilder} for a named reducer. */
export function defineReducer<S, V, Ctx extends Context = Context>(name: ReducerName): ReducerBuilder<S, V, Ctx> {
    return new ReducerBuilder<S, V, Ctx>(name);
}
