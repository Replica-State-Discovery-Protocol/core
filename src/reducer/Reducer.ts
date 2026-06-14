// src/reducer/Reducer.ts
import type { Address, ReducerName } from '../domain/address.js';
import type { Context } from '../domain/context.js';
import type { TranslatedState } from '../domain/state.js';
import { ReducerBuilder } from './internal/ReducerBuilder.js';
import type { Pipeline } from './pipeline/Pipeline.js';
import type { ExceptionFilter } from './pipeline/stages.js';

/** Derives the public `View` from the internal converged `State`. */
export interface Translator<State, View, Ctx> {
    translate(
        state: State | null,
        prev: State | null,
        ctx: Ctx,
    ): TranslatedState<View> | Promise<TranslatedState<View>>;
}

/**
 * An assembled, immutable reducer: three message-type pipelines (STATUS / SHARE / CLOSE),
 * a translator, and the per-stage exception filters. Built via {@link defineReducer}; the
 * engine drives it through `runStatus`/`runShare`/`runClose` + `translate`. `State` is the
 * internal converged state; `View` is what the translator emits and what crosses the wire.
 */
export class Reducer<State, View, Ctx extends Context> {
    constructor(
        readonly name: ReducerName,

        private readonly statusPipe: Pipeline<View, unknown, State, Ctx>,
        private readonly sharePipe: Pipeline<View, unknown, State, Ctx>,
        private readonly closePipe: Pipeline<Address, unknown, State, Ctx>,

        private readonly translator: Translator<State, View, Ctx>,

        readonly exceptionFilters: {
            status: ExceptionFilter<Ctx>[];
            share: ExceptionFilter<Ctx>[];
            close: ExceptionFilter<Ctx>[];
        },
    ) {}

    runStatus(batch: View[], ctx: Ctx, prev: State | null): Promise<State> {
        return this.statusPipe.run(batch, ctx, prev);
    }
    runShare(batch: View[], ctx: Ctx, prev: State | null): Promise<State> {
        return this.sharePipe.run(batch, ctx, prev);
    }
    runClose(batch: Address[], ctx: Ctx, prev: State | null): Promise<State> {
        return this.closePipe.run(batch, ctx, prev);
    }

    translate(state: State | null, prev: State | null, ctx: Ctx): Promise<TranslatedState<View>> {
        return Promise.resolve(this.translator.translate(state, prev, ctx));
    }
}

/** Entry point: start a fluent {@link ReducerBuilder} for a named reducer. */
export function defineReducer<State, View, Ctx extends Context = Context>(
    name: ReducerName,
): ReducerBuilder<State, View, Ctx> {
    return new ReducerBuilder<State, View, Ctx>(name);
}
