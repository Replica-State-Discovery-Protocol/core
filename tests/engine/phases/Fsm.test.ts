// tests/engine/phases/Fsm.test.ts
import { describe, expect, it } from 'vitest';

import { Fsm, Phase } from '../../../src/engine/phases/Fsm.js';

describe('Fsm', () => {
    it('starts INITIAL and walks INITIAL→DEBATE→IDLE', () => {
        const fsm = new Fsm();
        expect(fsm.phase).toBe(Phase.INITIAL);
        fsm.to(Phase.DEBATE);
        fsm.to(Phase.IDLE);
        expect(fsm.phase).toBe(Phase.IDLE);
    });

    it('rejects illegal transitions', () => {
        const fsm = new Fsm();
        expect(() => fsm.to(Phase.SHARE)).toThrow(); // INITIAL→SHARE illegal
    });

    it('allows IDLE↔CLOSE and IDLE→SHARE→IDLE', () => {
        const fsm = new Fsm();
        fsm.to(Phase.DEBATE);
        fsm.to(Phase.IDLE);
        fsm.to(Phase.SHARE);
        fsm.to(Phase.IDLE);
        fsm.to(Phase.CLOSE);
        fsm.to(Phase.IDLE);
        expect(fsm.phase).toBe(Phase.IDLE);
    });
});
