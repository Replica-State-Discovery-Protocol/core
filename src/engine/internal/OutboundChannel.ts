import type { Address, ReducerName } from '../../domain/address.js';
import type { ReducerPayload } from '../../domain/message.js';
import { MessageType } from '../../domain/message.js';
import type { Slan } from '../../slan/Slan.js';

interface TransportErrors {
    emitTransport(op: string, err: unknown): void;
}

/**
 * Wraps SLAN sends with the node's own identity and uniform transport-error routing,
 * so the orchestrator never repeats the broadcast/sendTo try-catch boilerplate.
 */
export class OutboundChannel {
    constructor(
        private readonly slan: Slan,
        private readonly from: Address,
        private readonly errors: TransportErrors,
    ) {}

    async hello(): Promise<void> {
        try {
            await this.slan.broadcast({ type: MessageType.Hello, from: this.from });
        } catch (err) {
            this.errors.emitTransport('broadcast', err);
        }
    }

    async share(payloads: Record<ReducerName, ReducerPayload>): Promise<void> {
        try {
            await this.slan.broadcast({ type: MessageType.Share, from: this.from, payloads });
        } catch (err) {
            this.errors.emitTransport('broadcast', err);
        }
    }

    async close(): Promise<void> {
        try {
            await this.slan.broadcast({ type: MessageType.Close, from: this.from, closed: this.from });
        } catch (err) {
            this.errors.emitTransport('broadcast', err);
        }
    }

    async status(to: Address, payloads: Record<ReducerName, ReducerPayload>): Promise<void> {
        try {
            await this.slan.sendTo(to, { type: MessageType.Status, from: this.from, payloads });
        } catch (err) {
            this.errors.emitTransport('sendTo', err);
        }
    }
}
