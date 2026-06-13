// tests/engine/schedule/RunQueue.test.ts
import { describe, expect, it } from 'vitest';

import { RunQueue } from '../../../src/engine/schedule/RunQueue.js';

const deferred = () => {
    let resolve!: () => void;
    const promise = new Promise<void>((r) => (resolve = r));
    return { promise, resolve };
};

describe('RunQueue', () => {
    it('runs serially and coalesces triggers arriving mid-run into one follow-up', async () => {
        const gates = [deferred(), deferred()];
        let started = 0;
        const q = new RunQueue(async () => {
            const gate = gates[started++];
            await gate?.promise;
        });

        q.trigger(); // starts run #1
        q.trigger(); // mid-run → marks dirty
        q.trigger(); // still dirty (coalesced)
        expect(started).toBe(1);

        gates[0]?.resolve(); // finish run #1 → should start exactly one follow-up
        await Promise.resolve();
        await Promise.resolve();
        expect(started).toBe(2);

        gates[1]?.resolve();
        await q.idle();
        expect(started).toBe(2); // no extra run
    });
});
