// src/engine/schedule/RunQueue.ts
export class RunQueue {
    private running = false;
    private dirty = false;
    private settled: Promise<void> = Promise.resolve();

    constructor(private readonly run: () => Promise<void>) {}

    trigger(): void {
        if (this.running) {
            this.dirty = true;
            return;
        }

        this.running = true;
        this.settled = this.loop();
    }

    /** Resolves when no run is in progress or pending. */
    async idle(): Promise<void> {
        await this.settled;
    }

    private async loop(): Promise<void> {
        try {
            do {
                this.dirty = false;
                await this.run();
            } while (this.dirty);
        } finally {
            this.running = false;
        }
    }
}
