import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Address } from '../../src/domain/address.js';
import type { Context } from '../../src/domain/context.js';
import type { WireMessage } from '../../src/domain/message.js';
import { MessageType } from '../../src/domain/message.js';
import { ErrorChannel } from '../../src/engine/channels/ErrorChannel.js';
import { OutboundChannel } from '../../src/engine/channels/OutboundChannel.js';
import { Coordinator } from '../../src/engine/Coordinator.js';
import { ReducerSlot } from '../../src/engine/state/ReducerSlot.js';
import { SlotRegistry } from '../../src/engine/state/SlotRegistry.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';
import { defineReducer, type Reducer } from '../../src/reducer/Reducer.js';
import type { Slan } from '../../src/slan/Slan.js';

type V = string[];
const union = (self: string): Aggregator<V, V, Context> => ({
    aggregate: (b) => [...new Set([self, ...b.flat()])].sort(),
});
const reducer = (): Reducer<unknown, unknown, Context> =>
    defineReducer<V, V, Context>('members')
        .status((p) => p.setAggregator(union('a')))
        .share((p) => p.setAggregator(union('a')))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({
            translate: (s, prev) => ({ value: s, changed: JSON.stringify(s) !== JSON.stringify(prev) }),
        }) as unknown as Reducer<unknown, unknown, Context>;

class RecordingSlan implements Slan {
    readonly address: Address = 'a';
    readonly sent: WireMessage[] = [];
    init(): Promise<void> {
        return Promise.resolve();
    }
    close(): Promise<void> {
        return Promise.resolve();
    }
    broadcast(msg: WireMessage): Promise<void> {
        this.sent.push(msg);
        return Promise.resolve();
    }
    sendTo(_t: Address, msg: WireMessage): Promise<void> {
        this.sent.push(msg);
        return Promise.resolve();
    }
    onMessage(): () => void {
        return () => undefined;
    }
    count(type: MessageType): number {
        return this.sent.filter((m) => m.type === type).length;
    }
}

const setup = () => {
    const clock = new FakeClock(0);
    const registry = new SlotRegistry<Context>();
    const r = reducer();
    registry.add(new ReducerSlot<Context>(r));
    const slan = new RecordingSlan();
    const errors = new ErrorChannel<Context>();
    const outbound = new OutboundChannel(slan, 'a', errors);
    const coordinator = new Coordinator<Context>({
        clock,
        registry,
        outbound,
        errors,
        self: 'a',
        debounce: { delayMs: 10, maxWaitMs: 50 },
        resyncIntervalMs: 1000,
    });
    return { clock, registry, slan, coordinator, r };
};

const drain = async (clock: FakeClock, coordinator: { settle(): Promise<void> }) => {
    for (let i = 0; i < 6; i++) {
        clock.advance(20);
        await coordinator.settle();
        await Promise.resolve();
    }
};

describe('Coordinator', () => {
    it('bootstraps: HELLO, then converges to self, broadcasts one SHARE, notifies observers', async () => {
        const { clock, registry, slan, coordinator, r } = setup();
        const snaps: unknown[] = [];
        coordinator.onConverged((s) => snaps.push(s));

        await coordinator.start();
        await drain(clock, coordinator);

        expect(registry.stateOf(r)?.value).toEqual(['a']);
        expect(slan.count(MessageType.Hello)).toBe(1);
        expect(slan.count(MessageType.Share)).toBe(1);
        expect(snaps.length).toBeGreaterThanOrEqual(1);

        await coordinator.stop();
    });

    it('scheduleSteady recomputes over Σ and broadcasts one SHARE on change', async () => {
        const { clock, registry, slan, coordinator, r } = setup();
        await coordinator.start();
        await drain(clock, coordinator);

        registry.get('members')?.ingestShare('peer', ['peer'], 1, clock.now());
        const sharesBefore = slan.count(MessageType.Share);
        coordinator.scheduleSteady();
        await drain(clock, coordinator);

        expect(registry.stateOf(r)?.value).toEqual(['a', 'peer']);
        expect(slan.count(MessageType.Share) - sharesBefore).toBe(1);

        await coordinator.stop();
    });

    it('periodically resyncs by re-broadcasting HELLO', async () => {
        const { clock, slan, coordinator } = setup();
        await coordinator.start();
        await drain(clock, coordinator);

        const helloBefore = slan.count(MessageType.Hello);
        clock.advance(1000); // reach resyncIntervalMs → resync fires
        await coordinator.settle();
        await drain(clock, coordinator);

        expect(slan.count(MessageType.Hello)).toBeGreaterThan(helloBefore);

        await coordinator.stop();
    });

    it('onConverged unsubscribe stops further notifications', async () => {
        const { clock, registry, coordinator } = setup();
        let calls = 0;
        const off = coordinator.onConverged(() => {
            calls += 1;
        });
        await coordinator.start();
        await drain(clock, coordinator);
        const afterStart = calls;
        expect(afterStart).toBeGreaterThanOrEqual(1);

        off();
        registry.get('members')?.ingestShare('peer', ['peer'], 1, clock.now());
        coordinator.scheduleSteady();
        await drain(clock, coordinator);
        expect(calls).toBe(afterStart); // no more after unsubscribe

        await coordinator.stop();
    });
});
