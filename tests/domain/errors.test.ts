// tests/domain/errors.test.ts
import { describe, expect, it } from 'vitest';

import { ConfigError, GuardRejected, PipelineError, RsdpError } from '../../src/errors.js';

describe('error hierarchy', () => {
    it('all errors extend RsdpError and carry a name', () => {
        const errs = [
            new ConfigError('c'),
            new GuardRejected('g'),
            new PipelineError('p', { reducer: 'r', stage: 'guard', messageType: 'SHARE' }),
        ];
        for (const e of errs) {
            expect(e).toBeInstanceOf(RsdpError);
            expect(e.name).toBe(e.constructor.name);
            expect(e.message.length).toBeGreaterThan(0);
        }
    });

    it('PipelineError exposes context', () => {
        const e = new PipelineError('boom', { reducer: 'cluster-members', stage: 'aggregator', messageType: 'SHARE' });
        expect(e.context).toEqual({ reducer: 'cluster-members', stage: 'aggregator', messageType: 'SHARE' });
    });
});
