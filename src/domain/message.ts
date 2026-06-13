import type { Address, ReducerName } from './address.js';

export enum MessageType {
    Hello = 'HELLO',
    Status = 'STATUS',
    Share = 'SHARE',
    Close = 'CLOSE',
}

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
