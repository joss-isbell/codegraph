import { afterEach, describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import CodeGraph, { getDatabasePath } from '../src/index';
import { createSelectedEmbeddedFixture, type SelectedEmbeddedFixture } from './helpers/selected-embedded-fixture';

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function eventually(assertion: () => void, timeout = 8000): Promise<void> {
  const end = Date.now() + timeout;
  while (true) {
    try { assertion(); return; } catch (error) {
      if (Date.now() >= end) throw error;
      await delay(60);
    }
  }
}

describe('selected embedded repositories through the real graph lifecycle', () => {
  let fixture: SelectedEmbeddedFixture | undefined;
  let cg: CodeGraph | undefined;
  const witnesses: fs.FSWatcher[] = [];
  afterEach(() => {
    for (const witness of witnesses.splice(0)) witness.close();
    cg?.close();
    cg = undefined;
    if (fixture && fixture.base !== process.env.T32_RETAINED) fixture.dispose();
    fixture = undefined;
  });
  const has = (name: string) => cg!.getNodesByName(name).length > 0;
  const dependencies = () => expect(cg!.getFileDependencies('hidden/source/entry.ts')).toEqual(
    expect.arrayContaining(['hidden/source/leaf.ts', 'hidden/deps/pkg/dep.ts']),
  );
  const calls = (leaf: string) => {
    const entry = cg!.getNodesByName('sourceEntry').find(node => node.kind === 'function');
    expect(entry).toBeDefined();
    expect(cg!.getCallees(entry!.id).map(({ node }) => node.name)).toEqual(
      expect.arrayContaining([leaf, 'dependencyFact']),
    );
  };

  it('retains an evaluated index across closed changes and actual reopen', async () => {
    const retained = process.env.T32_RETAINED;
    const checkpoint = retained && path.join(retained, 'baseline.json');
    if (process.env.T32_PHASE !== 'reopen') {
      fixture = createSelectedEmbeddedFixture(retained);
      // Pre-repair scanning can see ordinary visible child source. Closing the
      // session then hides it beneath a selected parent to exercise catch-up.
      fixture.write('.gitignore', '/hidden/deps/\n/hidden/unselected/\n/hidden/loose.ts\n');
      fixture.write('hidden/source/doomed.ts', 'export function closedDeletedFact() { return 23; }\n');
      cg = await CodeGraph.init(fixture.root, { index: true });
      expect(has('sourceLeaf')).toBe(true);
      expect(has('closedDeletedFact')).toBe(true);
      const baseline = {
        root: fixture.root,
        ordinary: cg.getFile('ordinary.ts'),
        dbIdentity: fs.statSync(getDatabasePath(fixture.root), { bigint: true }).ino.toString(),
        files: cg.getFiles().map(file => file.path).sort(),
      };
      cg.close(); cg = undefined;
      fixture.write('.gitignore', '/hidden/\n');
      fixture.write('hidden/source/leaf.ts', 'export function sourceLeafUpdated() { return 29; }\n');
      fixture.write('hidden/source/entry.ts', "import { sourceLeafUpdated } from './leaf';\nimport { dependencyFact } from '../deps/pkg/dep';\nexport function sourceEntry() { return sourceLeafUpdated() + dependencyFact(); }\n");
      fs.unlinkSync(path.join(fixture.root, 'hidden/source/doomed.ts'));
      fixture.write('hidden/source/closed-added.ts', 'export function closedAddedFact() { return 31; }\n');
      if (checkpoint) fs.writeFileSync(checkpoint, JSON.stringify(baseline, null, 2));
      if (process.env.T32_PHASE === 'baseline') return;
      cg = await CodeGraph.open(fixture.root, { sync: true });
      expect(cg.getFile('ordinary.ts')).toEqual(baseline.ordinary);
      expect(fs.statSync(getDatabasePath(fixture.root), { bigint: true }).ino.toString()).toBe(baseline.dbIdentity);
    } else {
      if (!checkpoint) throw new Error('T32_RETAINED required for evaluated-index reopen');
      const baseline = JSON.parse(fs.readFileSync(checkpoint, 'utf8'));
      const beforeOpen = fs.statSync(getDatabasePath(baseline.root), { bigint: true }).ino.toString();
      cg = await CodeGraph.open(baseline.root, { sync: true });
      expect(cg.getFile('ordinary.ts')).toEqual(baseline.ordinary);
      const afterOpen = fs.statSync(getDatabasePath(baseline.root), { bigint: true }).ino.toString();
      expect(afterOpen).toBe(beforeOpen);
      fs.writeFileSync(path.join(retained!, 'reopen-identity.json'), JSON.stringify({ beforeOpen, afterOpen }));
    }
    expect(has('sourceLeaf')).toBe(false);
    expect(has('sourceLeafUpdated')).toBe(true);
    expect(has('closedDeletedFact')).toBe(false);
    expect(has('closedAddedFact')).toBe(true);
    expect(has('dependencyFact')).toBe(true);
    expect(has('unrelatedFact')).toBe(true);
    expect(has('externalExcludedFact')).toBe(false);
    expect(has('unselectedFact')).toBe(false);
    dependencies();
    calls('sourceLeafUpdated');
  }, 30000);

  it.runIf(process.platform === 'win32')('refreshes source, future descendants and scope inputs through native Windows events', async () => {
    fixture = createSelectedEmbeddedFixture();
    cg = await CodeGraph.init(fixture.root, { index: true });
    expect(cg.getFiles().map(file => file.path).sort()).toEqual(fixture.expectedFiles);
    const errors: Error[] = [];
    expect(cg.watch({ debounceMs: 50, onSyncError: error => errors.push(error) })).toBe(true);
    await cg.waitUntilWatcherReady();
    dependencies();
    calls('sourceLeaf');
    let externalEvents = 0;
    witnesses.push(fs.watch(fixture.base, (_event, name) => {
      if (String(name) === 'external-excludes') externalEvents++;
    }));

    // This external input is outside the recursive project stream. No source
    // edit or manual sync may stand in for the rule event.
    fs.writeFileSync(fixture.externalExclude, 'external-ignored.ts\nleaf.ts\n');
    await eventually(() => expect(externalEvents).toBeGreaterThan(0));
    await eventually(() => expect(has('sourceLeaf')).toBe(false));
    fs.unlinkSync(fixture.externalExclude);
    await eventually(() => expect(has('sourceLeaf')).toBe(true));
    fs.writeFileSync(fixture.externalExclude, 'external-ignored.ts\n');
    await eventually(() => expect(has('externalExcludedFact')).toBe(false));

    fixture.write('hidden/source/future/deeper/new.ts', 'export function nativeFutureFact() { return 37; }\n');
    await eventually(() => expect(has('nativeFutureFact')).toBe(true));
    fixture.write('hidden/unselected/late.ts', 'export function nativeExcludedSibling() {}\n');
    fixture.write('hidden/source/leaf.ts', 'export function nativeEditedLeaf() { return 41; }\n');
    fixture.write('hidden/source/entry.ts', "import { nativeEditedLeaf } from './leaf';\nimport { dependencyFact } from '../deps/pkg/dep';\nexport function sourceEntry() { return nativeEditedLeaf() + dependencyFact(); }\n");
    await eventually(() => expect(has('nativeEditedLeaf')).toBe(true));
    await eventually(() => calls('nativeEditedLeaf'));

    fixture.write('hidden/source/nested/.gitignore', '*.skip.ts\n');
    await eventually(() => expect(has('negatedFact')).toBe(false));
    fixture.write('hidden/source/nested/.gitignore', '*.skip.ts\n!allowed.skip.ts\n');
    await eventually(() => expect(has('negatedFact')).toBe(true));
    expect(has('sourceLeaf')).toBe(false);
    expect(has('nativeExcludedSibling')).toBe(false);
    fs.unlinkSync(path.join(fixture.root, 'hidden/source/future/deeper/new.ts'));
    await eventually(() => expect(has('nativeFutureFact')).toBe(false));
    expect(cg.getFile('hidden/source/future/deeper/new.ts')).toBeNull();

    const rule = path.join(fixture.sourceRoot, '.gitignore');
    fs.writeFileSync(rule + '.replacement', 'root-ignored.ts\nsealed/\nleaf.ts\n');
    fs.renameSync(rule + '.replacement', rule);
    await eventually(() => expect(has('nativeEditedLeaf')).toBe(false));
    fs.unlinkSync(rule);
    await eventually(() => expect(has('rootIgnoredFact')).toBe(true));
    fs.writeFileSync(rule, 'root-ignored.ts\nsealed/\n');
    await eventually(() => expect(has('rootIgnoredFact')).toBe(false));
    await eventually(() => expect(has('nativeEditedLeaf')).toBe(true));

    fixture.write('hidden/source/.git/info/exclude', 'info-ignored.ts\nleaf.ts\n');
    await eventually(() => expect(has('nativeEditedLeaf')).toBe(false));
    fixture.write('hidden/source/.git/info/exclude', 'info-ignored.ts\n');
    await eventually(() => expect(has('nativeEditedLeaf')).toBe(true));

    const second = path.join(fixture.base, 'second-excludes');
    fs.writeFileSync(second, 'external-ignored.ts\nleaf.ts\n');
    fixture.git(fixture.sourceRoot, ['config', '--local', 'core.excludesFile', second]);
    await eventually(() => expect(has('nativeEditedLeaf')).toBe(false));
    fs.writeFileSync(second + '.replacement', 'external-ignored.ts\n');
    fs.renameSync(second + '.replacement', second);
    await eventually(() => expect(has('nativeEditedLeaf')).toBe(true));

    fixture.writeConfig(['hidden/deps/*/']);
    await eventually(() => expect(has('nativeEditedLeaf')).toBe(false));
    expect(has('dependencyFact')).toBe(true);
    fixture.writeConfig();
    await eventually(() => expect(has('nativeEditedLeaf')).toBe(true));
    fs.unlinkSync(path.join(fixture.root, 'codegraph.json'));
    await eventually(() => expect(has('dependencyFact')).toBe(false));
    fixture.writeConfig();
    await eventually(() => expect(has('dependencyFact')).toBe(true));
    await eventually(() => calls('nativeEditedLeaf'));
    expect(has('unrelatedFact')).toBe(true);
    expect(errors).toEqual([]);
  }, 60000);
});
