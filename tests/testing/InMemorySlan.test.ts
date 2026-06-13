// tests/testing/InMemorySlan.test.ts
import { describe, expect, it } from 'vitest';

import type { WireMessage } from '../../src/domain/message.js';
import { MessageType } from '../../src/domain/message.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

const hello = (from: string): WireMessage => ({ type: MessageType.Hello, from });

describe('InMemorySlan', () => {
    it('broadcast reaches every other node but not the sender', async () => {
        const bus = new InMemoryBus();
        const a = new InMemorySlan('a', bus);
        const b = new InMemorySlan('b', bus);
        const c = new InMemorySlan('c', bus);
        const seenB: string[] = [];
        const seenC: string[] = [];
        b.onMessage((m) => seenB.push(m.from));
        c.onMessage((m) => seenC.push(m.from));
        await Promise.all([a.init(), b.init(), c.init()]);

        await a.broadcast(hello('a'));
        expect(seenB).toEqual(['a']);
        expect(seenC).toEqual(['a']);
    });

    it('sendTo reaches only the target', async () => {
        const bus = new InMemoryBus();
        const a = new InMemorySlan('a', bus);
        const b = new InMemorySlan('b', bus);
        const c = new InMemorySlan('c', bus);
        const seenB: string[] = [];
        const seenC: string[] = [];
        b.onMessage((m) => seenB.push(m.from));
        c.onMessage((m) => seenC.push(m.from));
        await Promise.all([a.init(), b.init(), c.init()]);

        await a.sendTo('b', hello('a'));
        expect(seenB).toEqual(['a']);
        expect(seenC).toEqual([]);
    });
});
