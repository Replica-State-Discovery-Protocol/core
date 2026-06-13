import type { Address, ReducerName } from './address.js';

export type MessageType = 'HELLO' | 'STATUS' | 'SHARE' | 'CLOSE';

export interface ReducerPayload {
    value: unknown;
    version: number;
}

export interface WireMessage {
    type: MessageType;
    from: Address;
    payloads?: Record<ReducerName, ReducerPayload>; // STATUS / SHARE composite
    closed?: Address; // CLOSE
}
