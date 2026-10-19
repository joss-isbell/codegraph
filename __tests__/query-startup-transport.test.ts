import { describe, expect, it, vi } from 'vitest';
import { captureQueryStartup } from '../src/mcp/query-policy';
import { captureMcpQueryStartup, daemonQueryEnvironment } from '../src/mcp/query-startup-transport';

const metadata = 'CODEGRAPH_INTERNAL_QUERY_STARTUP_CPU_V1';
const root = 'captured-project';

describe('captured query startup across a native candidate environment', () => {
  it('preserves original absence and CPU rather than later inherited controls', () => {
    const startup = captureQueryStartup({}, 3);
    const inherited = { CODEGRAPH_QUERY_MODE: 'unlimited', CODEGRAPH_QUERY_POOL_SIZE: '20',
      CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited', [metadata]: 'stale', OTHER: 'retained' };
    const child = daemonQueryEnvironment(inherited, startup);
    expect(child.CODEGRAPH_QUERY_MODE).toBeUndefined();
    expect(child.CODEGRAPH_QUERY_POOL_SIZE).toBeUndefined();
    expect(child.CODEGRAPH_QUERY_BUSY_TIMEOUT_MS).toBeUndefined();
    expect(child.OTHER).toBe('retained');
    expect(JSON.parse(child[metadata]!)).toEqual({ protocol: 1, cpuCount: 3 });
    expect(inherited.CODEGRAPH_QUERY_MODE).toBe('unlimited');
    expect(inherited[metadata]).toBe('stale');
    child.CODEGRAPH_DAEMON_INTERNAL = '1';
    const cpu = vi.fn(() => 99);
    const captured = captureMcpQueryStartup(child, cpu, true, root);
    expect(captured).toEqual({ requested: {}, cpuCount: 3 });
    expect(cpu).not.toHaveBeenCalled();
    expect(child[metadata]).toBeUndefined();
    expect(Object.isFrozen(captured.requested)).toBe(true);
  });

  it('retains raw spelling but removes all transported controls from descendants', () => {
    const startup = captureQueryStartup({ CODEGRAPH_QUERY_MODE: ' default ',
      CODEGRAPH_QUERY_POOL_SIZE: '020', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: ' 47000 ' }, 3);
    const child = daemonQueryEnvironment({}, startup);
    child.CODEGRAPH_DAEMON_INTERNAL = '1';
    const captured = captureMcpQueryStartup(child, () => { throw new Error('child CPU observed'); }, true, root);
    expect(captured).toEqual({ requested: { mode: ' default ', poolSize: '020', busyTimeoutMs: ' 47000 ' }, cpuCount: 3 });
    for (const name of [metadata, 'CODEGRAPH_QUERY_MODE', 'CODEGRAPH_QUERY_POOL_SIZE', 'CODEGRAPH_QUERY_BUSY_TIMEOUT_MS']) {
      expect(child[name]).toBeUndefined();
    }
  });

  it.each([undefined, 'broken', '{}', '{"protocol":2,"cpuCount":3}',
    '{"protocol":1,"cpuCount":-1}', '{"protocol":1,"cpuCount":1.5}',
    '{"protocol":1,"cpuCount":9007199254740992}', '{"protocol":1,"cpuCount":3,"extra":true}'])
  ('refuses qualified invalid CPU metadata %s without observing CPU or retaining raw controls', value => {
    const env: NodeJS.ProcessEnv = { CODEGRAPH_DAEMON_INTERNAL: '1', CODEGRAPH_QUERY_MODE: 'unlimited', [metadata]: value };
    const cpu = vi.fn(() => 99);
    expect(() => captureMcpQueryStartup(env, cpu, true, root)).toThrow(expect.objectContaining({
      code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root,
    }));
    expect(cpu).not.toHaveBeenCalled();
    expect(env[metadata]).toBeUndefined();
    expect(env.CODEGRAPH_QUERY_MODE).toBeUndefined();
  });

  it('turns a qualified malformed public control into terminal unknown after scrubbing', () => {
    const env = { CODEGRAPH_DAEMON_INTERNAL: '1', CODEGRAPH_QUERY_MODE: 'unlimited x',
      [metadata]: '{"protocol":1,"cpuCount":3}' };
    expect(() => captureMcpQueryStartup(env, () => 99, true, root)).toThrow(expect.objectContaining({
      code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown',
    }));
    expect(env.CODEGRAPH_QUERY_MODE).toBeUndefined();
  });

  it.each([{ marker: '1', ipc: false }, { marker: 'true', ipc: true }, { marker: undefined, ipc: true }])
  ('ignores and scrubs stale metadata outside exact native qualification %j', ({ marker, ipc }) => {
    const env: NodeJS.ProcessEnv = { CODEGRAPH_DAEMON_INTERNAL: marker,
      CODEGRAPH_QUERY_POOL_SIZE: '20', [metadata]: 'broken' };
    const cpu = vi.fn(() => 7);
    expect(captureMcpQueryStartup(env, cpu, ipc, root)).toEqual({ requested: { poolSize: '20' }, cpuCount: 7 });
    expect(cpu).toHaveBeenCalledTimes(1);
    expect(env[metadata]).toBeUndefined();
  });

  it('scrubs and refuses a disconnected native candidate before observing CPU', () => {
    const env: NodeJS.ProcessEnv = { CODEGRAPH_DAEMON_INTERNAL: '1', CODEGRAPH_QUERY_MODE: 'unlimited',
      CODEGRAPH_QUERY_POOL_SIZE: '20', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: '1000', [metadata]: 'broken' };
    const cpu = vi.fn(() => 97);
    expect(() => captureMcpQueryStartup(env, cpu, false, root, true)).toThrow(expect.objectContaining({
      code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root,
    }));
    expect(cpu).not.toHaveBeenCalled();
    for (const name of [metadata, 'CODEGRAPH_QUERY_MODE', 'CODEGRAPH_QUERY_POOL_SIZE', 'CODEGRAPH_QUERY_BUSY_TIMEOUT_MS']) {
      expect(env[name]).toBeUndefined();
    }
  });
});
