import { RsdpError } from '../../errors.js';

export enum Phase {
    INITIAL = 'INITIAL',
    DEBATE = 'DEBATE',
    IDLE = 'IDLE',
    SHARE = 'SHARE',
    CLOSE = 'CLOSE',
}

const TRANSITIONS: Record<Phase, Phase[]> = {
    // Bootstrap always begins with the DEBATE round; nothing else may run first.
    [Phase.INITIAL]: [Phase.DEBATE],
    // IDLE → DEBATE: a periodic resync re-enters DEBATE to re-gather perspectives.
    [Phase.DEBATE]: [Phase.IDLE],
    [Phase.IDLE]: [Phase.DEBATE, Phase.SHARE, Phase.CLOSE],
    [Phase.SHARE]: [Phase.IDLE],
    [Phase.CLOSE]: [Phase.IDLE],
};

export class Fsm {
    private current: Phase = Phase.INITIAL;

    get phase(): Phase {
        return this.current;
    }

    to(next: Phase): void {
        if (!TRANSITIONS[this.current].includes(next)) {
            throw new RsdpError(`illegal phase transition ${this.current} → ${next}`);
        }
        this.current = next;
    }
}
