import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Address } from '../../src/domain/address.js';
import type { Context } from '../../src/domain/context.js';
import type { WireMessage } from '../../src/domain/message.js';
import { MessageType } from '../../src/domain/message.js';
import { OutboundChannel } from '../../src/engine/channels/OutboundChannel.js';
import { InboundRouter } from '../../src/engine/InboundRouter.js';
import { ReducerSlot } from '../../src/engine/state/ReducerSlot.js';
import { SlotRegistry } from '../../src/engine/state/SlotRegistry.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';
import { defineReducer, type Reducer } from '../../src/reducer/Reducer.js';
import type { Slan } from '../../src/slan/Slan.js';

type V = string[];
const union = (self: string): Aggregator<V, V, Context> => ({
    aggregate: (b) => [...new Set([self, ...b.flat()])].sort(),
});
const reducer = (name: string): Reducer<unknown, unknown, Context> =>
    defineReducer<V, V, Context>(name)
        .status((p) => p.setAggregator(union('a')))
        .share((p) => p.setAggregator(union('a')))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({ translate: (s) => ({ value: s, changed: true }) }) as unknown as Reducer<
        unknown,
        unknown,
        Context
    >;

class RecordingSlan implements Slan {
    readonly address: Address = 'a';
    readonly sent: { target?: Address; msg: WireMessage }[] = [];
    init(): Promise<void> {
        return Promise.resolve();
    }
    close(): Promise<void> {
        return Promise.resolve();
    }
    broadcast(msg: WireMessage): Promise<void> {
        this.sent.push({ msg });
        return Promise.resolve();
    }
    sendTo(target: Address, msg: WireMessage): Promise<void> {
        this.sent.push({ target, msg });
        return Promise.resolve();
    }
    onMessage(): () => void {
        return () => undefined;
    }
}

const recordingScheduler = () => ({
    debate: 0,
    steady: 0,
    close: 0,
    scheduleDebate(): void {
        this.debate += 1;
    },
    scheduleSteady(): void {
        this.steady += 1;
    },
    scheduleClose(): void {
        this.close += 1;
    },
});

const setup = () => {
    const registry = new SlotRegistry<Context>();
    registry.add(new ReducerSlot<Context>(reducer('members')));
    const slan = new RecordingSlan();
    const outbound = new OutboundChannel(slan, 'a', { emitTransport: () => undefined });
    const scheduler = recordingScheduler();
    const router = new InboundRouter<Context>(registry, outbound, new FakeClock(0), scheduler);
    return { registry, slan, scheduler, router };
};

describe('InboundRouter', () => {
    it('replies to HELLO with our composite STATUS, without scheduling convergence', async () => {
        const { slan, scheduler, router } = setup();
        await router.handle({ type: MessageType.Hello, from: 'peer' }, 'peer');

        expect(slan.sent).toEqual([
            {
                target: 'peer',
                msg: { type: MessageType.Status, from: 'a', payloads: { members: { value: null, version: 0 } } },
            },
        ]);
        expect(scheduler.debate).toBe(0);
        expect(scheduler.steady).toBe(0);
    });

    it('STATUS with a real payload schedules a DEBATE round', async () => {
        const { scheduler, router } = setup();
        await router.handle(
            { type: MessageType.Status, from: 'peer', payloads: { members: { value: ['peer'], version: 1 } } },
            'peer',
        );
        expect(scheduler.debate).toBe(1);
        expect(scheduler.steady).toBe(0);
    });

    it('ignores null payloads and unknown reducers (no scheduling)', async () => {
        const { scheduler, router } = setup();
        await router.handle(
            { type: MessageType.Status, from: 'peer', payloads: { members: { value: null, version: 1 } } },
            'peer',
        );
        await router.handle(
            { type: MessageType.Status, from: 'peer', payloads: { ghost: { value: ['x'], version: 1 } } },
            'peer',
        );
        expect(scheduler.debate).toBe(0);
    });

    it('SHARE schedules steady once; a duplicate (equal version) does not', async () => {
        const { scheduler, router } = setup();
        const share: WireMessage = {
            type: MessageType.Share,
            from: 'peer',
            payloads: { members: { value: ['peer'], version: 1 } },
        };
        await router.handle(share, 'peer');
        await router.handle(share, 'peer'); // version-gated no-op
        expect(scheduler.steady).toBe(1);
    });

    it('CLOSE of a known peer buffers the departure and schedules a CLOSE round', async () => {
        const { scheduler, router } = setup();
        await router.handle(
            { type: MessageType.Share, from: 'peer', payloads: { members: { value: ['peer'], version: 1 } } },
            'peer',
        );
        const steadyBefore = scheduler.steady; // the SHARE above scheduled one steady
        await router.handle({ type: MessageType.Close, from: 'peer', closed: 'peer' }, 'peer');
        expect(scheduler.close).toBe(1);
        expect(scheduler.steady).toBe(steadyBefore); // CLOSE no longer rides the steady path
    });

    it('CLOSE of an unknown peer is ignored (no CLOSE round)', async () => {
        const { scheduler, router } = setup();
        await router.handle({ type: MessageType.Close, from: 'ghost', closed: 'ghost' }, 'ghost');
        expect(scheduler.close).toBe(0);
    });
});
