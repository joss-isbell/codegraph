import { describe, expect, it } from 'vitest';
import { resolveQueryPolicy, resolveLegacyQueryPolicy, type QueryExecutionMode } from '../src/mcp/query-policy';

describe('query policy compatibility and independent controls', () => {
  it('resolves the unlimited preset with independent finite overrides', () => {
    const policy = resolveQueryPolicy({ mode: ' unlimited ', poolSize: '00023' }, 'direct', 8);
    expect(policy.requested).toEqual({ mode: ' unlimited ', poolSize: '00023' });
    expect(policy.concurrency).toEqual({ kind: 'finite', maximum: 23 });
    expect(policy.deadline).toEqual({ kind: 'absent' });
    expect(policy.retention).toBe('retire-idle');
    expect(policy.classification).toBe('custom');
    expect(policy.defaults.concurrency).toEqual({ kind: 'finite', maximum: 2 });
    expect(Object.isFrozen(policy)).toBe(true);
    expect(Object.isFrozen(policy.requested)).toBe(true);
  });
  it('keeps prospective daemon defaults separate from the direct engine', () => {
    expect(resolveLegacyQueryPolicy({}, 'direct', 64).concurrency).toEqual({ kind: 'finite', maximum: 2 });
    expect(resolveLegacyQueryPolicy({}, 'shared-daemon', 64).concurrency).toEqual({ kind: 'finite', maximum: 16 });
  });
  it.each([
    ['direct', 2, 'finite'], ['writable-fallback', 2, 'finite'],
    ['shared-daemon', 7, 'finite'], ['read-only-fallback', undefined, 'inapplicable'],
  ] as const)('preserves %s defaults for unset and explicit default mode', (mode, maximum, deadlineKind) => {
    for (const requested of [{}, { mode: 'default' }, { mode: ' default ' }]) {
      const policy = resolveQueryPolicy(requested, mode, 8);
      expect(policy.concurrency).toEqual(maximum ? { kind: 'finite', maximum } : { kind: 'disabled' });
      expect(policy.deadline.kind).toBe(deadlineKind);
      expect(policy.retention).toBe('ordinary');
      expect(policy.classification).toBe('default');
    }
  });
  it.each(['direct', 'writable-fallback', 'shared-daemon', 'read-only-fallback'] as QueryExecutionMode[])
  ('resolves independent overrides and retirement in %s', (mode) => {
    // All preset/override combinations: literals are the settled contract,
    // independent of the resolver's own comparisons/default computations.
    for (const preset of [undefined, 'default', 'unlimited']) {
      for (const poolSize of [undefined, '0', '23', 'unlimited']) {
        for (const busyTimeoutMs of [undefined, '1000', 'unlimited']) {
          const policy = resolveQueryPolicy({ mode: preset, poolSize, busyTimeoutMs }, mode, 8);
          const disabled = poolSize === '0' || (mode === 'read-only-fallback' && poolSize === undefined && preset !== 'unlimited');
          expect(policy.retention).toBe(preset === 'unlimited' || poolSize === 'unlimited' ? 'retire-idle' : 'ordinary');
          if (disabled) {
            expect(policy.concurrency).toEqual({ kind: 'disabled' });
            expect(policy.deadline).toEqual({ kind: 'inapplicable' });
            expect(policy.classification).toBe(poolSize !== undefined || busyTimeoutMs !== undefined || preset === 'unlimited' ? 'custom' : 'default');
          } else {
            const unbounded = poolSize === 'unlimited' || (poolSize === undefined && preset === 'unlimited');
            expect(policy.concurrency).toEqual(unbounded ? { kind: 'unlimited' } : { kind: 'finite', maximum: poolSize === '23' ? 23 : mode === 'shared-daemon' ? 7 : 2 });
            const absent = busyTimeoutMs === 'unlimited' || (busyTimeoutMs === undefined && preset === 'unlimited');
            expect(policy.deadline).toEqual(absent ? { kind: 'absent' } : { kind: 'finite', milliseconds: busyTimeoutMs === '1000' ? 1000 : 45000 });
            expect(policy.classification).toBe(unbounded && absent ? 'unlimited' : preset === 'unlimited' || poolSize !== undefined || busyTimeoutMs !== undefined ? 'custom' : 'default');
          }
        }
      }
    }
  });
  it.each(['', ' ', 'Default', 'Unlimited', 'DEFAULT', 'unlimited x', '0'])('rejects mode %j with actionable guidance', (mode) => {
    expect(() => resolveQueryPolicy({ mode }, 'direct', 8)).toThrow(`CODEGRAPH_QUERY_MODE value ${JSON.stringify(mode)}; expected default or unlimited`);
  });
  // These spellings and fallback cases belonged to the existing public
  // Number/floor parser. Revised #29 adds controls, not a syntax migration.
  it.each([
    ['', 2], [' ', 0], ['-1', 7], ['+1', 1], ['1.0', 1], ['.5', 0], ['1e3', 1000], ['0x10', 16],
    ['NaN', 7], ['Infinity', 7], ['-Infinity', 7], ['abc', 7], ['UNLIMITED', 7], ['1_000', 7],
    ['9007199254740992', 7], ['99999999999999999999999999999999999999', 7],
  ] as const)('preserves legacy pool size %j as %i without a preset', (poolSize, maximum) => {
    expect(resolveQueryPolicy({ poolSize }, 'direct', 8).concurrency)
      .toEqual(maximum === 0 ? { kind: 'disabled' } : { kind: 'finite', maximum });
  });
  it.each([
    ['', 45000], [' ', 45000], ['0', 45000], ['999', 45000], ['-1000', 45000], ['+1000', 1000],
    ['1000.0', 1000], ['1e3', 1000], ['0x1000', 4096], ['NaN', 45000], ['Infinity', 45000],
    ['abc', 45000], ['UNLIMITED', 45000], ['2147483648', 45000], ['9007199254740991', 45000],
  ] as const)('preserves legacy timeout %j as %i without scheduling an overflowing timer', (busyTimeoutMs, milliseconds) => {
    expect(resolveQueryPolicy({ poolSize: '1', busyTimeoutMs }, 'direct', 8).deadline)
      .toEqual({ kind: 'finite', milliseconds });
  });
  it('accepts exact numeric boundaries, trimmed sentinels and leading zeroes', () => {
    expect(resolveQueryPolicy({ poolSize: ' 0009007199254740991 ', busyTimeoutMs: '02147483647' }, 'direct', 8).concurrency).toEqual({ kind: 'finite', maximum: 9007199254740991 });
    expect(resolveQueryPolicy({ poolSize: '1', busyTimeoutMs: ' 001000 ' }, 'direct', 8).deadline).toEqual({ kind: 'finite', milliseconds: 1000 });
    expect(resolveQueryPolicy({ poolSize: ' unlimited ', busyTimeoutMs: ' unlimited ' }, 'direct', 8).classification).toBe('unlimited');
    expect(resolveQueryPolicy({ mode: 'unlimited', poolSize: '000' }, 'direct', 8).deadline).toEqual({ kind: 'inapplicable' });
  });
  it('preserves permissive public parsing without activating the new preset', () => {
    expect(resolveLegacyQueryPolicy({ mode: 'unlimited', poolSize: '1e3', busyTimeoutMs: '1000.9' }, 'direct', 8)).toMatchObject({
      source: 'legacy', preset: 'default', classification: 'custom', concurrency: { kind: 'finite', maximum: 1000 }, deadline: { kind: 'finite', milliseconds: 1000 }, retention: 'ordinary',
    });
    expect(resolveLegacyQueryPolicy({ mode: 'unlimited', poolSize: 'unlimited', busyTimeoutMs: 'unlimited' }, 'direct', 8)).toMatchObject({
      concurrency: { kind: 'unlimited' }, deadline: { kind: 'absent' }, retention: 'retire-idle',
    });
    expect(resolveLegacyQueryPolicy({ poolSize: ' ' }, 'direct', 8).deadline.kind).toBe('inapplicable');
    expect(resolveLegacyQueryPolicy({ poolSize: '4' }, 'read-only-fallback', 8)).toMatchObject({ concurrency: { kind: 'finite', maximum: 4 }, deadline: { kind: 'finite', milliseconds: 45000 } });
    for (const poolSize of [undefined, '', 'malformed', '0']) {
      expect(resolveLegacyQueryPolicy({ poolSize }, 'read-only-fallback', 8)).toMatchObject({ concurrency: { kind: 'disabled' }, deadline: { kind: 'inapplicable' } });
    }
  });
});
