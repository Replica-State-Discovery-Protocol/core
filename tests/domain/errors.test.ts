// tests/domain/errors.test.ts
import { describe, expect, it } from 'vitest';

import { MessageType } from '../../src/domain/message.js';
import { ConfigError, GuardRejected, PipelineError, PipelineStage, RsdpError } from '../../src/errors.js';

describe('error hierarchy', () => {
    it('all errors extend RsdpError and carry a name', () => {
        const errs = [
            new ConfigError('c'),
            new GuardRejected('g'),
            new PipelineError('p', { reducer: 'r', stage: PipelineStage.Guard, messageType: MessageType.Share }),
        ];
        for (const e of errs) {
            expect(e).toBeInstanceOf(RsdpError);
            expect(e.name).toBe(e.constructor.name);
            expect(e.message.length).toBeGreaterThan(0);
        }
    });

    it('PipelineError exposes context', () => {
        const e = new PipelineError('boom', {
            reducer: 'cluster-members',
            stage: PipelineStage.Aggregator,
            messageType: MessageType.Share,
        });
        expect(e.context).toEqual({
            reducer: 'cluster-members',
            stage: PipelineStage.Aggregator,
            messageType: MessageType.Share,
        });
    });
});
