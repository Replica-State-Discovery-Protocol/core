import { describe, expect, it } from 'vitest';

import { DebateBuffer } from '../../../src/engine/state/DebateBuffer.js';

describe('DebateBuffer', () => {
    it('keeps the latest payload per peer and snapshots values', () => {
        const b = new DebateBuffer<string>();
        b.set('a', 'first');
        b.set('a', 'second'); // overwrite
        b.set('b', 'x');
        expect(b.snapshot().sort()).toEqual(['second', 'x']);
    });

    it('clears', () => {
        const b = new DebateBuffer<string>();
        b.set('a', 'x');
        b.clear();
        expect(b.snapshot()).toEqual([]);
    });
});
