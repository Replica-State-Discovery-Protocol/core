import { describe, expect, it } from 'vitest';

import type { Address, ReducerName } from '../../../src/domain/address.js';
import type { ReducerPayload, WireMessage } from '../../../src/domain/message.js';
import { MessageType } from '../../../src/domain/message.js';
import { OutboundChannel } from '../../../src/engine/channels/OutboundChannel.js';
import type { Slan } from '../../../src/slan/Slan.js';

class RecordingSlan implements Slan {
    readonly address: Address = 'self';
    readonly sent: { kind: 'broadcast' | 'sendTo'; target?: Address; msg: WireMessage }[] = [];
    failBroadcast = false;
    failSendTo = false;

    init(): Promise<void> {
        return Promise.resolve();
    }
    close(): Promise<void> {
        return Promise.resolve();
    }
    broadcast(msg: WireMessage): Promise<void> {
        if (this.failBroadcast) return Promise.reject(new Error('broadcast down'));
        this.sent.push({ kind: 'broadcast', msg });
        return Promise.resolve();
    }
    sendTo(target: Address, msg: WireMessage): Promise<void> {
        if (this.failSendTo) return Promise.reject(new Error('sendTo down'));
        this.sent.push({ kind: 'sendTo', target, msg });
        return Promise.resolve();
    }
    onMessage(): () => void {
        return () => undefined;
    }
}

const recordingErrors = () => {
    const calls: { op: string; err: unknown }[] = [];
    return { calls, emitTransport: (op: string, err: unknown) => calls.push({ op, err }) };
};

const payloads: Record<ReducerName, ReducerPayload> = { r: { value: ['x'], version: 3 } };

describe('OutboundChannel', () => {
    it('stamps the node identity on hello/share/close and targets status', async () => {
        const slan = new RecordingSlan();
        const out = new OutboundChannel(slan, 'self', recordingErrors());

        await out.hello();
        await out.share(payloads);
        await out.close();
        await out.status('peer', payloads);

        expect(slan.sent).toEqual([
            { kind: 'broadcast', msg: { type: MessageType.Hello, from: 'self' } },
            { kind: 'broadcast', msg: { type: MessageType.Share, from: 'self', payloads } },
            { kind: 'broadcast', msg: { type: MessageType.Close, from: 'self', closed: 'self' } },
            { kind: 'sendTo', target: 'peer', msg: { type: MessageType.Status, from: 'self', payloads } },
        ]);
    });

    it('routes a broadcast failure to emitTransport("broadcast") instead of throwing', async () => {
        const slan = new RecordingSlan();
        slan.failBroadcast = true;
        const errors = recordingErrors();
        const out = new OutboundChannel(slan, 'self', errors);

        await expect(out.share(payloads)).resolves.toBeUndefined();
        expect(errors.calls).toHaveLength(1);
        expect(errors.calls[0]?.op).toBe('broadcast');
        expect((errors.calls[0]?.err as Error).message).toBe('broadcast down');
        expect(slan.sent).toHaveLength(0);
    });

    it('routes a sendTo failure to emitTransport("sendTo")', async () => {
        const slan = new RecordingSlan();
        slan.failSendTo = true;
        const errors = recordingErrors();
        const out = new OutboundChannel(slan, 'self', errors);

        await out.status('peer', payloads);
        expect(errors.calls).toHaveLength(1);
        expect(errors.calls[0]?.op).toBe('sendTo');
        expect(errors.calls[0]?.err).toBeInstanceOf(Error);
    });
});
