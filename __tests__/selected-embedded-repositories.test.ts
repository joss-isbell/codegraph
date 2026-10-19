import { afterEach, describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import { scanDirectory, scanDirectoryAsync } from '../src/extraction';
import CodeGraph from '../src/index';
import { createSelectedEmbeddedFixture, SelectedEmbeddedFixture } from './helpers/selected-embedded-fixture';

describe('selected embedded repositories below a non-Git parent', () => {
  let fixture: SelectedEmbeddedFixture | undefined;
  let graph: CodeGraph | undefined;
  afterEach(() => {
    graph?.close();
    fixture?.dispose();
  });

  it('scans selected literal and directory-pattern roots without admitting hidden siblings or ignored content', async () => {
    fixture = createSelectedEmbeddedFixture();
    // Git is an independent oracle for nested negation and unreachable rules.
    expect(fixture.gitIgnored('nested/ignored.skip.ts')).toBe(true);
    expect(fixture.gitIgnored('nested/allowed.skip.ts')).toBe(false);
    expect(fixture.gitIgnored('sealed/reopened/target.ts')).toBe(true);
    expect(fixture.gitIgnored('info-ignored.ts')).toBe(true);
    expect(fixture.gitIgnored('external-ignored.ts')).toBe(true);
    expect(scanDirectory(fixture.root).sort()).toEqual(fixture.expectedFiles);
    expect((await scanDirectoryAsync(fixture.root)).sort()).toEqual(fixture.expectedFiles);
  });

  it('keeps scoped sync aligned with Git for future nested files and rule replacements', async () => {
    fixture = createSelectedEmbeddedFixture();
    graph = await CodeGraph.init(fixture.root);
    await graph.indexAll();
    expect(graph.getFiles().map((file) => file.path).sort()).toEqual(fixture.expectedFiles);
    // Establish the same scoped matcher before the future path exists.
    await graph.sync({ paths: ['hidden/source/leaf.ts'] });
    expect(fixture.gitIgnored('nested/future.skip.ts')).toBe(true);
    fixture.write('hidden/source/nested/future.skip.ts', 'export function futureIgnoredFact() {}\n');
    expect(fixture.gitIgnored('nested/future.skip.ts')).toBe(true);
    expect(fixture.gitIgnored('sealed/reopened/target.ts')).toBe(true);
    await graph.sync({ paths: ['hidden/source/nested/future.skip.ts', 'hidden/source/sealed/reopened/target.ts'] });
    expect(graph.getFiles().map((file) => file.path).sort()).toEqual(fixture.expectedFiles);
    expect(graph.searchNodes('futureIgnoredFact')).toHaveLength(0);
    expect(graph.searchNodes('unreachableFact')).toHaveLength(0);

    // A replacement changes child eligibility, not the parent's config/mtime.
    fixture.write('hidden/source/nested/.gitignore', '*.skip.ts\n!allowed.skip.ts\n!future.skip.ts\n');
    expect(fixture.gitIgnored('nested/future.skip.ts')).toBe(false);
    await graph.sync({ paths: ['hidden/source/nested/future.skip.ts'] });
    expect(graph.getFiles().map((file) => file.path).sort()).toEqual([
      ...fixture.expectedFiles, 'hidden/source/nested/future.skip.ts',
    ].sort());
    expect(graph.getNodesByName('futureIgnoredFact').map((node) => node.name)).toContain('futureIgnoredFact');
  });

  it('rejects an unreadable selected Git inventory without removing prior indexed files or facts', async () => {
    fixture = createSelectedEmbeddedFixture();
    graph = await CodeGraph.init(fixture.root);
    await graph.indexAll();
    expect(graph.getFiles().map((file) => file.path).sort()).toEqual(fixture.expectedFiles);
    const unrelatedId = graph.getNodesByName('unrelatedFact').find((node) => node.name === 'unrelatedFact')!.id;
    const entry = graph.getNodesInFile('hidden/source/entry.ts').find((node) => node.name === 'sourceEntry')!;
    expect(graph.getCallees(entry.id).map((result) => result.node.name).sort()).toEqual(['dependencyFact', 'sourceLeaf']);
    // Real corrupt Git metadata, not a mocked collaborator or injected failure.
    fixture.write('hidden/source/.git/index', 'not a Git index\n');
    await expect(graph.sync()).rejects.toThrow();
    expect(graph.getFiles().map((file) => file.path).sort()).toEqual(fixture.expectedFiles);
    expect(graph.getNodesByName('unrelatedFact').find((node) => node.name === 'unrelatedFact')?.id).toBe(unrelatedId);
    expect(graph.getCallees(entry.id).map((result) => result.node.name).sort()).toEqual(['dependencyFact', 'sourceLeaf']);
  });

  it('rejects capped selected discovery without removing prior indexed files or facts', async () => {
    fixture = createSelectedEmbeddedFixture();
    graph = await CodeGraph.init(fixture.root);
    await graph.indexAll();
    expect(graph.getFiles().map((file) => file.path).sort()).toEqual(fixture.expectedFiles);
    const unrelatedId = graph.getNodesByName('unrelatedFact').find((node) => node.name === 'unrelatedFact')!.id;
    const entry = graph.getNodesInFile('hidden/source/entry.ts').find((node) => node.name === 'sourceEntry')!;
    expect(graph.getCallees(entry.id).map((result) => result.node.name).sort()).toEqual(['dependencyFact', 'sourceLeaf']);
    // Only non-Git directories within the existing directory-pattern prefix.
    // The bounded search must leave queued entries, regardless of readdir order.
    for (let index = 0; index < 2001; index++) {
      fs.mkdirSync(path.join(fixture.root, 'hidden', 'deps', `a-cap-${String(index).padStart(4, '0')}`));
    }
    await expect(graph.sync()).rejects.toThrow();
    expect(graph.getFiles().map((file) => file.path).sort()).toEqual(fixture.expectedFiles);
    expect(graph.getNodesByName('unrelatedFact').find((node) => node.name === 'unrelatedFact')?.id).toBe(unrelatedId);
    expect(graph.getCallees(entry.id).map((result) => result.node.name).sort()).toEqual(['dependencyFact', 'sourceLeaf']);
  });
});
