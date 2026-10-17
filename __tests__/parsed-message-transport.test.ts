import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  ErrorCodes,
  ParsedMessageTransport,
  type JsonRpcNotification,
  type JsonRpcRequest,
} from '../src/mcp/transport';

const transports: ParsedMessageTransport[] = [];

function captureTransport(): {
  transport: ParsedMessageTransport;
  output: Array<Record<string, unknown>>;
} {
  const output: Array<Record<string, unknown>> = [];
  const transport = new ParsedMessageTransport((line) => {
    output.push(JSON.parse(line) as Record<string, unknown>);
    return true;
  });
  transports.push(transport);
  return { transport, output };
}

function within<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}`)), 1000);
  });
  return Promise.race([promise, deadline]).finally(() => clearTimeout(timer));
}

afterEach(() => {
  for (const transport of transports.splice(0)) transport.stop();
  vi.useRealTimers();
});

describe('ParsedMessageTransport', () => {
  it('delivers an original initialize and notifications without opening another input reader', async () => {
    const events = ['data', 'end', 'error', 'close', 'readable'] as const;
    const listenerCounts = events.map((event) => process.stdin.listenerCount(event));
    const { transport, output } = captureTransport();
    const received: Array<JsonRpcRequest | JsonRpcNotification> = [];
    transport.start(async (message) => { received.push(message); });
    const initialize = {
      jsonrpc: '2.0' as const,
      id: 17,
      method: 'initialize',
      params: {
        rootUri: 'file:///workspace/alpha',
        capabilities: { roots: { listChanged: true } },
        clientInfo: { name: 'adapter-client', version: '1' },
      },
    };
    const initialized = { jsonrpc: '2.0' as const, method: 'notifications/initialized' };

    await transport.accept(initialize);
    await transport.accept(initialized);

    expect(received).toEqual([initialize, initialized]);
    expect(output).toEqual([]);
    expect(events.map((event) => process.stdin.listenerCount(event))).toEqual(listenerCounts);
    transport.stop();
    expect(events.map((event) => process.stdin.listenerCount(event))).toEqual(listenerCounts);
  });

  it.each([
    null,
    false,
    7,
    'not a JSON-RPC object',
    [],
    {},
    { jsonrpc: '1.0', method: 'ping' },
    { jsonrpc: '2.0', method: 7 },
  ])('rejects invalid parsed input %j through the ordinary JSON-RPC error response', async (input) => {
    const { transport, output } = captureTransport();
    const received: unknown[] = [];
    transport.start(async (message) => { received.push(message); });

    await transport.accept(input);

    expect(output).toEqual([{
      jsonrpc: '2.0',
      id: null,
      error: {
        code: ErrorCodes.InvalidRequest,
        message: 'Invalid Request: not a valid JSON-RPC 2.0 message',
      },
    }]);
    expect(received).toEqual([]);
  });

  it('reports request handler failure while leaving failed notifications without a reply', async () => {
    const { transport, output } = captureTransport();
    transport.start(async () => { throw new Error('application failed'); });

    await transport.accept({ jsonrpc: '2.0', id: 'call-1', method: 'tools/call' });
    await transport.accept({ jsonrpc: '2.0', method: 'notifications/initialized' });

    expect(output).toEqual([{
      jsonrpc: '2.0',
      id: 'call-1',
      error: { code: ErrorCodes.InternalError, message: 'Internal error: application failed' },
    }]);
  });

  it('accepts a roots response while the request handler is awaiting that same exchange', async () => {
    const { transport, output } = captureTransport();
    transport.start(async (message) => {
      const roots = await transport.request('roots/list');
      transport.sendResult((message as JsonRpcRequest).id, roots);
    });
    const dispatch = transport.accept({ jsonrpc: '2.0', id: 21, method: 'tools/call' });
    try {
      expect(output).toEqual([{ jsonrpc: '2.0', id: 'cg-srv-1', method: 'roots/list' }]);
      await within(transport.accept({
        jsonrpc: '2.0',
        id: 'cg-srv-1',
        result: { roots: [{ uri: 'file:///workspace/beta', name: 'beta' }] },
      }), 'client roots response dispatch');
      await within(dispatch, 'original request completion');

      expect(output[1]).toEqual({
        jsonrpc: '2.0',
        id: 21,
        result: { roots: [{ uri: 'file:///workspace/beta', name: 'beta' }] },
      });
    } finally {
      transport.stop();
      await within(dispatch, 'roots handler cleanup');
    }
  });

  it('correlates independent outbound IDs, including error and late or unknown responses', async () => {
    const { transport, output } = captureTransport();
    const received: unknown[] = [];
    transport.start(async (message) => { received.push(message); });
    const roots = transport.request('roots/list');
    const denied = transport.request('client/permission', { root: 'beta' });
    const settled = Promise.allSettled([roots, denied]);

    expect(output).toEqual([
      { jsonrpc: '2.0', id: 'cg-srv-1', method: 'roots/list' },
      { jsonrpc: '2.0', id: 'cg-srv-2', method: 'client/permission', params: { root: 'beta' } },
    ]);
    await transport.accept({ jsonrpc: '2.0', id: 'unknown', result: { ignored: true } });
    await transport.accept({
      jsonrpc: '2.0', id: 'cg-srv-2', error: { code: -1, message: 'permission denied' },
    });
    await transport.accept({ jsonrpc: '2.0', id: 'cg-srv-1', result: { roots: [] } });
    await transport.accept({ jsonrpc: '2.0', id: 'cg-srv-1', result: { roots: ['late'] } });

    const results = await settled;
    expect(results[0]).toEqual({ status: 'fulfilled', value: { roots: [] } });
    expect(results[1]).toEqual({ status: 'rejected', reason: new Error('permission denied') });
    expect(received).toEqual([]);
  });

  it('times out an unanswered request without letting its late response resolve a later request', async () => {
    vi.useFakeTimers();
    const { transport, output } = captureTransport();
    transport.start(async () => {});
    const first = transport.request('roots/list', undefined, 25);
    const expired = expect(first).rejects.toThrow('Timed out after 25ms waiting for "roots/list" response');
    await vi.advanceTimersByTimeAsync(25);
    await expired;
    const next = transport.request('roots/list', undefined, 25);
    const nextResult = expect(next).resolves.toEqual({ roots: [{ uri: 'file:///new' }] });

    await transport.accept({ jsonrpc: '2.0', id: 'cg-srv-1', result: { roots: ['old'] } });
    await transport.accept({
      jsonrpc: '2.0', id: 'cg-srv-2', result: { roots: [{ uri: 'file:///new' }] },
    });

    await nextResult;
    expect(output.map((message) => message.id)).toEqual(['cg-srv-1', 'cg-srv-2']);
  });

  it('closes once, rejects outstanding requests and ignores incoming work after stop', async () => {
    const { transport, output } = captureTransport();
    const received: unknown[] = [];
    const closed: string[] = [];
    transport.start(async (message) => { received.push(message); });
    transport.onClose(() => { closed.push('first'); throw new Error('closed observer failed'); });
    transport.onClose(() => { closed.push('second'); });
    const pending = Promise.allSettled([
      transport.request('roots/list'),
      transport.request('client/permission'),
    ]);
    transport.stop();
    transport.stop();
    transport.onClose(() => { closed.push('after-close'); });
    await transport.accept({ jsonrpc: '2.0', id: 1, method: 'tools/call' });

    expect(await pending).toEqual([
      { status: 'rejected', reason: new Error('Transport stopped') },
      { status: 'rejected', reason: new Error('Transport stopped') },
    ]);
    expect(closed).toEqual(['first', 'second', 'after-close']);
    expect(received).toEqual([]);
    expect(transport.sendResult(1, { late: true })).toBe(false);
    expect(output).toHaveLength(2);
  });

  it('accepts cancellation while a previous handler remains active instead of serializing dispatch', async () => {
    const { transport, output } = captureTransport();
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    let active = false;
    let cancelledWhileActive = false;
    transport.start(async (message) => {
      if (message.method === 'tools/call') {
        active = true;
        await held;
        active = false;
        return;
      }
      if (message.method === 'notifications/cancelled') {
        cancelledWhileActive = active;
        release();
      }
    });
    const running = transport.accept({ jsonrpc: '2.0', id: 'work', method: 'tools/call' });
    const cancellation = transport.accept({
      jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: 'work' },
    });
    try {
      await within(cancellation, 'cancellation during running handler');
      await within(running, 'cancelled handler completion');
      expect(cancelledWhileActive).toBe(true);
      expect(output).toEqual([]);
    } finally {
      release();
      transport.stop();
      await within(Promise.allSettled([running, cancellation]), 'concurrent dispatch cleanup');
    }
  });

  it('returns the outer writer delivery result without adding a second newline', async () => {
    const lines: string[] = [];
    const transport = new ParsedMessageTransport((line) => { lines.push(line); return false; });
    transports.push(transport);
    transport.start(async () => {});

    expect(transport.sendResult('out', { ready: true })).toBe(false);
    expect(lines).toEqual(['{"jsonrpc":"2.0","id":"out","result":{"ready":true}}']);
  });
});

