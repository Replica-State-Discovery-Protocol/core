import type { Clock } from '../../clock/Clock.js';
import type { DebounceConfig } from './Debouncer.js';
import { Debouncer } from './Debouncer.js';
import { Fsm, Phase } from './Fsm.js';

/** The phases the scheduler executes work in (everything except the at-rest IDLE/INITIAL). */
export type WorkPhase = Phase.DEBATE | Phase.SHARE | Phase.CLOSE;

const WORK_PHASES: readonly WorkPhase[] = [Phase.DEBATE, Phase.SHARE, Phase.CLOSE];

/** The aggregation each work phase executes when entered. */
export type PhaseHandlers = Record<WorkPhase, () => Promise<void>>;

export interface PhaseSchedulerOptions {
    clock: Clock;
    debounce: DebounceConfig;
    handlers: PhaseHandlers;
}

/**
 * Gates phase-specific work behind the protocol FSM. Reception (buffering incoming data
 * into Σ / the DEBATE buffer / the departed set) is never gated — that happens in the
 * router. What this gates is the *execution* of an aggregation: each phase's handler runs
 * only while the FSM is in that phase.
 *
 * A `request(phase)` is:
 * - **debounced** — a burst of requests coalesces into one run (each phase has its own
 *   δ/D_max window);
 * - **deduplicated** — a phase already scheduled (window open, queued, or executing) is
 *   not scheduled twice (see {@link isScheduled}); and
 * - **deferred** — a request that lands while another phase is executing waits, then runs
 *   once the FSM returns to IDLE.
 *
 * A single-flight drain loop runs at most one phase at a time, returning to IDLE between
 * phases, so the bootstrap `INITIAL → DEBATE` ordering and all `IDLE ↔ work` transitions
 * stay legal without any caller poking the FSM directly.
 */
export class PhaseScheduler {
    private readonly fsm = new Fsm();
    private readonly handlers: PhaseHandlers;
    private readonly debouncers: Record<WorkPhase, Debouncer>;
    private readonly pending = new Set<WorkPhase>();

    private draining = false;
    private settled: Promise<void> = Promise.resolve();

    constructor(opts: PhaseSchedulerOptions) {
        this.handlers = opts.handlers;
        this.debouncers = {
            [Phase.DEBATE]: new Debouncer(opts.clock, opts.debounce, () => this.onReady(Phase.DEBATE)),
            [Phase.SHARE]: new Debouncer(opts.clock, opts.debounce, () => this.onReady(Phase.SHARE)),
            [Phase.CLOSE]: new Debouncer(opts.clock, opts.debounce, () => this.onReady(Phase.CLOSE)),
        };
    }

    get phase(): Phase {
        return this.fsm.phase;
    }

    /** Request a work phase: debounced, deduplicated, deferred until runnable. */
    request(phase: WorkPhase): void {
        this.debouncers[phase].notifyChange();
    }

    /**
     * Whether `phase` is already scheduled in any sense — its debounce window is open, it
     * is queued to run, or it is executing now. Lets a periodic trigger (resync) skip
     * stacking a second identical request instead of dropping or duplicating it.
     */
    isScheduled(phase: WorkPhase): boolean {
        return this.debouncers[phase].armed || this.pending.has(phase) || this.fsm.phase === phase;
    }

    /** Resolve once no phase is executing. Armed-but-unfired windows fire on clock advance. */
    settle(): Promise<void> {
        return this.settled;
    }

    stop(): void {
        for (const phase of WORK_PHASES) this.debouncers[phase].cancel();
        this.pending.clear();
    }

    private onReady(phase: WorkPhase): void {
        this.pending.add(phase);
        // Mid-work (or not yet bootstrapped): a running drain will pick this up when it
        // next returns to IDLE; a not-yet-started drain begins as soon as it is runnable.
        if (this.draining || !this.runnable()) return;
        this.draining = true;
        this.settled = this.drain();
    }

    /**
     * Whether the drain may start a phase now. IDLE accepts any work phase; INITIAL accepts
     * only the bootstrap DEBATE, so a SHARE/CLOSE that arrives before the first debate waits
     * rather than forcing an illegal `INITIAL → SHARE/CLOSE`.
     */
    private runnable(): boolean {
        if (this.fsm.phase === Phase.IDLE) return true;
        if (this.fsm.phase === Phase.INITIAL) return this.pending.has(Phase.DEBATE);
        return false;
    }

    private async drain(): Promise<void> {
        try {
            while (this.runnable() && this.pending.size > 0) {
                const phase = this.takeNext();
                this.fsm.to(phase);
                await this.handlers[phase]();
                this.fsm.to(Phase.IDLE);
            }
        } finally {
            this.draining = false;
        }
    }

    /**
     * Pop the highest-priority pending phase. Order is DEBATE → CLOSE → SHARE: a re-gather
     * runs before a departure (so CLOSE has the last word over a DEBATE that might re-admit
     * a departed peer from stale Σ), and a steady SHARE runs last. Each requested phase runs
     * exactly once; a redundant SHARE after a DEBATE is a harmless no-op (Σ unchanged).
     */
    private takeNext(): WorkPhase {
        if (this.pending.has(Phase.DEBATE)) {
            this.pending.delete(Phase.DEBATE);
            return Phase.DEBATE;
        }
        if (this.pending.has(Phase.CLOSE)) {
            this.pending.delete(Phase.CLOSE);
            return Phase.CLOSE;
        }
        this.pending.delete(Phase.SHARE);
        return Phase.SHARE;
    }
}
