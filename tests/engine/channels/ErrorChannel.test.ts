import { describe, expect, it, vi } from 'vitest';

import type { Context } from '../../../src/domain/context.js';
import { MessageType } from '../../../src/domain/message.js';
import { ErrorChannel } from '../../../src/engine/channels/ErrorChannel.js';
import { GuardRejected, PipelineError, PipelineStage, RsdpError, SlanError } from '../../../src/errors.js';
import type { ExceptionFilter } from '../../../src/reducer/pipeline/stages.js';
import type { Reducer } from '../../../src/reducer/Reducer.js';

const ctx: Context = { self: 'me' };

// ErrorChannel only reads `name` and `exceptionFilters` off the reducer, so a
// lightweight structural fake keeps these tests focused on the channel itself.
const fakeReducer = (filters: {
    status?: ExceptionFilter<Context>[];
    share?: ExceptionFilter<Context>[];
    close?: ExceptionFilter<Context>[];
}): Reducer<unknown, unknown, Context> =>
    ({
        name: 'r',
        exceptionFilters: { status: filters.status ?? [], share: filters.share ?? [], close: filters.close ?? [] },
    }) as unknown as Reducer<unknown, unknown, Context>;

describe('ErrorChannel', () => {
    it('delivers emitted errors to every subscriber until unsubscribed', () => {
        const ch = new ErrorChannel<Context>();
        const a = vi.fn();
        const b = vi.fn();
        ch.subscribe(a);
        const offB = ch.subscribe(b);

        ch.emitTransport('broadcast', new Error('x'));
        expect(a).toHaveBeenCalledTimes(1);
        expect(b).toHaveBeenCalledTimes(1);

        offB();
        ch.emitTransport('broadcast', new Error('y'));
        expect(a).toHaveBeenCalledTimes(2);
        expect(b).toHaveBeenCalledTimes(1);
    });

    it('wraps a raw transport failure as a SlanError naming the op', () => {
        const ch = new ErrorChannel<Context>();
        const seen: RsdpError[] = [];
        ch.subscribe((e) => seen.push(e));

        ch.emitTransport('sendTo', new Error('boom'));
        expect(seen[0]).toBeInstanceOf(SlanError);
        expect(seen[0]?.message).toBe('SLAN sendTo failed: boom');
    });

    it('passes an existing RsdpError through transport without re-wrapping', () => {
        const ch = new ErrorChannel<Context>();
        const seen: RsdpError[] = [];
        ch.subscribe((e) => seen.push(e));

        const original = new GuardRejected('nope');
        ch.emitTransport('broadcast', original);
        expect(seen[0]).toBe(original);
    });

    it('wraps a pipeline failure as a PipelineError carrying reducer/stage/messageType', async () => {
        const ch = new ErrorChannel<Context>();
        const seen: RsdpError[] = [];
        ch.subscribe((e) => seen.push(e));

        await ch.emitPipeline(fakeReducer({}), MessageType.Share, new Error('agg blew up'), ctx);

        const err = seen[0];
        expect(err).toBeInstanceOf(PipelineError);
        expect((err as PipelineError).context).toEqual({
            reducer: 'r',
            stage: PipelineStage.Aggregator,
            messageType: MessageType.Share,
        });
        expect(err?.message).toBe('agg blew up');
    });

    it('offers the failure to the matching-stage filters before escalating', async () => {
        const handle = vi.fn();
        const otherStage = vi.fn();
        const ch = new ErrorChannel<Context>();
        const escalated: RsdpError[] = [];
        ch.subscribe((e) => escalated.push(e));

        await ch.emitPipeline(
            fakeReducer({ share: [{ handle }], status: [{ handle: otherStage }] }),
            MessageType.Share,
            new Error('x'),
            ctx,
        );

        expect(handle).toHaveBeenCalledTimes(1);
        expect(handle).toHaveBeenCalledWith(expect.any(PipelineError), ctx);
        expect(otherStage).not.toHaveBeenCalled(); // wrong stage
        expect(escalated).toHaveLength(1); // still escalated after the filter
    });

    it('still escalates when a filter throws', async () => {
        const ch = new ErrorChannel<Context>();
        const escalated: RsdpError[] = [];
        ch.subscribe((e) => escalated.push(e));

        const throwingFilter: ExceptionFilter<Context> = {
            handle: () => {
                throw new Error('filter failed');
            },
        };
        await ch.emitPipeline(fakeReducer({ share: [throwingFilter] }), MessageType.Share, new Error('x'), ctx);

        expect(escalated).toHaveLength(1);
        expect(escalated[0]).toBeInstanceOf(PipelineError);
    });
});
