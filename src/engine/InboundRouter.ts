import type { Clock } from '../clock/Clock.js';
import type { Address } from '../domain/address.js';
import type { Context } from '../domain/context.js';
import type { ReducerPayload, WireMessage } from '../domain/message.js';
import { MessageType } from '../domain/message.js';
import type { OutboundChannel } from './channels/OutboundChannel.js';
import type { ReducerSlot } from './state/ReducerSlot.js';
import type { SlotRegistry } from './state/SlotRegistry.js';

/** What {@link InboundRouter} pokes after applying a message, to drive convergence. */
export interface ConvergenceScheduler {
    scheduleDebate(): void;
    scheduleSteady(): void;
    scheduleClose(): void;
}

/**
 * Routes inbound wire messages to state updates and replies, then pokes convergence —
 * no convergence math of its own:
 * - HELLO  → reply with our composite view as STATUS
 * - STATUS → fill each reducer's DEBATE buffer    → scheduleDebate
 * - SHARE  → version-gate each reducer's Σ slot    → scheduleSteady
 * - CLOSE  → buffer the departed peer per reducer  → scheduleClose
 */
export class InboundRouter<Ctx extends Context> {
    constructor(
        private readonly registry: SlotRegistry<Ctx>,
        private readonly outbound: OutboundChannel,
        private readonly clock: Clock,
        private readonly scheduler: ConvergenceScheduler,
    ) {}

    async handle(msg: WireMessage, from: Address): Promise<void> {
        switch (msg.type) {
            case MessageType.Hello:
                // Reply with our current view as STATUS (full-consensus mode).
                await this.outbound.status(from, this.registry.composite());
                return;
            case MessageType.Status:
                this.ingest(
                    msg,
                    (slot, p) => slot.ingestStatus(from, p.value, msg.inc),
                    () => this.scheduler.scheduleDebate(),
                );
                return;
            case MessageType.Share:
                this.ingest(
                    msg,
                    (slot, p) => slot.ingestShare(from, p.value, p.version, this.clock.now(), msg.inc),
                    () => this.scheduler.scheduleSteady(),
                );
                return;
            case MessageType.Close: {
                const closed = msg.closed ?? from;
                // `msg.inc` is the SENDER's lifetime. It only describes the departing node
                // when a node announces its own departure — which is the only CLOSE the
                // engine emits. A relayed CLOSE about a third party carries no usable ι, so
                // it stays ungated rather than being gated against the wrong node's epoch.
                const closedInc = closed === from ? msg.inc : undefined;
                let buffered = false;
                for (const slot of this.registry.values()) if (slot.ingestClose(closed, closedInc)) buffered = true;
                if (buffered) this.scheduler.scheduleClose();
                return;
            }
        }
    }

    private ingest(
        msg: WireMessage,
        apply: (slot: ReducerSlot<Ctx>, payload: ReducerPayload) => boolean,
        schedule: () => void,
    ): void {
        if (!msg.payloads) return;
        let changed = false;
        for (const [name, payload] of Object.entries(msg.payloads)) {
            const slot = this.registry.get(name);
            if (!slot) continue;
            // A peer that has not yet translated a value emits `value: null` in its
            // composite (e.g. a STATUS reply to HELLO before its DEBATE has run). Such
            // empty contributions must not be ingested, or `null` leaks into reducer
            // batches and pollutes the converged state forever.
            if (payload.value === null) continue;
            // Only treat the message as a real change when the slot actually updated
            // (stale/duplicate SHAREs return false and must NOT trigger a re-run).
            if (apply(slot, payload)) changed = true;
        }
        if (changed) schedule();
    }
}
