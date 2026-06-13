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

export enum PipelineStage {
    Middleware = 'middleware',
    Guard = 'guard',
    Interceptor = 'interceptor',
    Normalizer = 'normalizer',
    Aggregator = 'aggregator',
    Translator = 'translator',
}

export interface PipelineErrorContext {
    reducer: string;
    stage: PipelineStage;
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
