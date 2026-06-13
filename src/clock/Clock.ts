export type TimerHandle = number;

export interface Clock {
    now(): number;
    setTimer(fn: () => void, ms: number): TimerHandle;
    clearTimer(handle: TimerHandle): void;
}

export class SystemClock implements Clock {
    now(): number {
        return Date.now();
    }
    setTimer(fn: () => void, ms: number): TimerHandle {
        return setTimeout(fn, ms) as unknown as TimerHandle;
    }
    clearTimer(handle: TimerHandle): void {
        clearTimeout(handle);
    }
}

interface Scheduled {
    id: TimerHandle;
    fireAt: number;
    fn: () => void;
}

export class FakeClock implements Clock {
    private current: number;
    private seq = 0;
    private timers: Scheduled[] = [];

    constructor(start = 0) {
        this.current = start;
    }

    now(): number {
        return this.current;
    }

    setTimer(fn: () => void, ms: number): TimerHandle {
        const id = ++this.seq;
        this.timers.push({ id, fireAt: this.current + Math.max(0, ms), fn });
        return id;
    }

    clearTimer(handle: TimerHandle): void {
        this.timers = this.timers.filter((t) => t.id !== handle);
    }

    /** Advance virtual time, firing every timer whose deadline is reached, in order. */
    advance(ms: number): void {
        const target = this.current + ms;
        for (;;) {
            const next = this.timers
                .filter((t) => t.fireAt <= target)
                .sort((a, b) => a.fireAt - b.fireAt || a.id - b.id)[0];
            if (!next) break;
            this.timers = this.timers.filter((t) => t.id !== next.id);
            this.current = next.fireAt;
            next.fn();
        }
        this.current = target;
    }
}
