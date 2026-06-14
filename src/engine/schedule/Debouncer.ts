// src/engine/schedule/Debouncer.ts
import type { Clock, TimerHandle } from '../../clock/Clock.js';

export interface DebounceConfig {
    delayMs: number; // δ — quiet gap before firing
    maxWaitMs: number; // D_max — hard cap since first change in a window
}

export class Debouncer {
    private firstChangeAt: number | null = null;
    private timer: TimerHandle | null = null;

    constructor(
        private readonly clock: Clock,
        private readonly config: DebounceConfig,
        private readonly onFire: () => void,
    ) {}

    notifyChange(): void {
        const now = this.clock.now();
        this.firstChangeAt ??= now;
        const deadline = Math.min(now + this.config.delayMs, this.firstChangeAt + this.config.maxWaitMs);

        if (this.timer !== null) this.clock.clearTimer(this.timer);

        this.timer = this.clock.setTimer(() => this.fire(), Math.max(0, deadline - now));
    }

    cancel(): void {
        if (this.timer !== null) this.clock.clearTimer(this.timer);
        this.timer = null;
        this.firstChangeAt = null;
    }

    private fire(): void {
        this.timer = null;
        this.firstChangeAt = null;
        this.onFire();
    }
}
