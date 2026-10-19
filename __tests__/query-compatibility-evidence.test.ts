import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { saveCompatibilityEvidence } from './query-compatibility-host';

describe('compatibility evidence destination', () => {
  let fixture: string;
  let previousOutput: string | undefined;
  const facts = { primary: '', cleanup: [{ phase: 'native-close', error: null }] };

  beforeEach(() => {
    fixture = fs.mkdtempSync(path.join(os.tmpdir(), 'cg-compatibility-evidence-'));
    previousOutput = process.env.VALIDATION_OUT;
    delete process.env.VALIDATION_OUT;
  });

  afterEach(() => {
    if (previousOutput === undefined) delete process.env.VALIDATION_OUT;
    else process.env.VALIDATION_OUT = previousOutput;
    fs.rmSync(fixture, { recursive: true, force: true });
  });

  it('retains the final aggregate at the ordinary caller destination', () => {
    const ordinary = path.join(fixture, 'ordinary.evidence.json');
    saveCompatibilityEvidence('canonical-name', facts, ordinary);
    expect(JSON.parse(fs.readFileSync(ordinary, 'utf8'))).toEqual(facts);
  });

  it('uses the qualified canonical destination when a collector is supplied', () => {
    const collector = path.join(fixture, 'collector');
    const ordinary = path.join(fixture, 'ordinary.evidence.json');
    fs.mkdirSync(collector);
    process.env.VALIDATION_OUT = collector;
    saveCompatibilityEvidence('canonical-name', facts, ordinary);
    expect(JSON.parse(fs.readFileSync(path.join(collector, 'canonical-name.json'), 'utf8'))).toEqual(facts);
    expect(fs.existsSync(ordinary)).toBe(false);
  });

  it('rejects a supplied invalid collector without falling back or changing its bytes', () => {
    const collector = path.join(fixture, 'collector-is-a-file');
    const ordinary = path.join(fixture, 'ordinary.evidence.json');
    fs.writeFileSync(collector, 'independent collector sentinel\n');
    process.env.VALIDATION_OUT = collector;
    expect(() => saveCompatibilityEvidence('canonical-name', facts, ordinary)).toThrow();
    expect(fs.readFileSync(collector, 'utf8')).toBe('independent collector sentinel\n');
    expect(fs.existsSync(ordinary)).toBe(false);
  });

  it('preserves optional two-argument callers without a collector', () => {
    saveCompatibilityEvidence('supplemental-only', facts);
    expect(fs.readdirSync(fixture)).toEqual([]);
  });
});
