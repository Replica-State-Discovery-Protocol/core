import type { MessageType } from './domain/message.js';

export class RsdpError extends Error {
    constructor(message: string) {
        super(message);
        this.name = new.target.name;
    }
}

export class ConfigError extends RsdpError {}

export class GuardRejected extends RsdpError {}

export class SlanError extends RsdpError {}

export interface PipelineErrorContext {
    reducer: string;
    stage: 'middleware' | 'guard' | 'interceptor' | 'normalizer' | 'aggregator' | 'translator';
    messageType: MessageType;
}

export class PipelineError extends RsdpError {
    constructor(
        message: string,
        public readonly context: PipelineErrorContext,
    ) {
        super(message);
    }
}
