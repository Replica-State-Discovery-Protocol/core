import type { Context } from '../../domain/context.js';
import { MessageType } from '../../domain/message.js';
import { PipelineError, PipelineStage, RsdpError, SlanError } from '../../errors.js';
import type { ExceptionFilter } from '../../reducer/pipeline/stages.js';
import type { Reducer } from '../../reducer/Reducer.js';
import type { Unsubscribe } from '../../slan/Slan.js';

/**
 * Owns the engine's `onError` subscribers and the policy for turning raw failures
 * into `RsdpError`s before escalating them. Transport failures become `SlanError`s;
 * reducer-pipeline failures become `PipelineError`s and are first offered to the
 * reducer's exception filters.
 */
export class ErrorChannel<Ctx extends Context> {
    private readonly cbs = new Set<(err: RsdpError) => void>();

    subscribe(cb: (err: RsdpError) => void): Unsubscribe {
        this.cbs.add(cb);
        return () => this.cbs.delete(cb);
    }

    private emit(err: RsdpError): void {
        for (const cb of this.cbs) cb(err);
    }

    /** Route a SLAN transport failure to subscribers instead of letting it float. */
    emitTransport(op: string, err: unknown): void {
        this.emit(
            err instanceof RsdpError
                ? err
                : new SlanError(`SLAN ${op} failed: ${err instanceof Error ? err.message : String(err)}`),
        );
    }

    /**
     * Wrap a reducer-pipeline failure, give the reducer's exception filters a chance to
     * handle it, then escalate to subscribers (a filter that throws falls through).
     */
    async emitPipeline(
        reducer: Reducer<unknown, unknown, Ctx>,
        stage: MessageType,
        err: unknown,
        ctx: Ctx,
    ): Promise<void> {
        const wrapped =
            err instanceof RsdpError
                ? err
                : new PipelineError(err instanceof Error ? err.message : String(err), {
                      reducer: reducer.name,
                      stage: PipelineStage.Aggregator,
                      messageType: stage,
                  });

        for (const filter of this.filtersFor(reducer, stage)) {
            try {
                await filter.handle(wrapped, ctx);
            } catch {
                // Filter rejected/failed — fall back to the global escalation.
            }
        }
        this.emit(wrapped);
    }

    private filtersFor(reducer: Reducer<unknown, unknown, Ctx>, stage: MessageType): ExceptionFilter<Ctx>[] {
        switch (stage) {
            case MessageType.Status:
                return reducer.exceptionFilters.status;
            case MessageType.Share:
                return reducer.exceptionFilters.share;
            case MessageType.Close:
                return reducer.exceptionFilters.close;
            default:
                return [];
        }
    }
}
