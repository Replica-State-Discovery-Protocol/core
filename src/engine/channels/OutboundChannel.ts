import type { Address, ReducerName } from '../../domain/address.js';
import type { ReducerPayload } from '../../domain/message.js';
import { MessageType } from '../../domain/message.js';
import type { IncarnationValue } from '../../incarnation/Incarnation.js';
import type { Slan } from '../../slan/Slan.js';

interface TransportErrors {
    emitTransport(op: string, err: unknown): void;
}

/**
 * Wraps SLAN sends with the node's own identity and incarnation plus uniform
 * transport-error routing, so the orchestrator never repeats the broadcast/sendTo
 * try-catch boilerplate. Every outbound message is stamped with `ι`, including the ones
 * no gate reads today — a receiver that learns to gate them should not have to wait for
 * senders to be upgraded.
 */
export class OutboundChannel {
    constructor(
        private readonly slan: Slan,
        private readonly from: Address,
        private readonly errors: TransportErrors,
        private readonly inc: IncarnationValue,
    ) {}

    async hello(): Promise<void> {
        try {
            await this.slan.broadcast({ type: MessageType.Hello, from: this.from, inc: this.inc });
        } catch (err) {
            this.errors.emitTransport('broadcast', err);
        }
    }

    async share(payloads: Record<ReducerName, ReducerPayload>): Promise<void> {
        try {
            await this.slan.broadcast({ type: MessageType.Share, from: this.from, inc: this.inc, payloads });
        } catch (err) {
            this.errors.emitTransport('broadcast', err);
        }
    }

    async close(): Promise<void> {
        try {
            await this.slan.broadcast({
                type: MessageType.Close,
                from: this.from,
                inc: this.inc,
                closed: this.from,
            });
        } catch (err) {
            this.errors.emitTransport('broadcast', err);
        }
    }

    async status(to: Address, payloads: Record<ReducerName, ReducerPayload>): Promise<void> {
        try {
            await this.slan.sendTo(to, { type: MessageType.Status, from: this.from, inc: this.inc, payloads });
        } catch (err) {
            this.errors.emitTransport('sendTo', err);
        }
    }
}
