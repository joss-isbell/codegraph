import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';

/** A single real non-Git workspace, shared by scanning and lifecycle coverage. */
export function createSelectedEmbeddedFixture(container?: string) {
  const base = container ?? fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-selected-'));
  const root = path.join(base, 'workspace');
  if (fs.existsSync(root)) throw new Error(`Fixture already exists: ${root}`);
  const sourceRoot = path.join(root, 'hidden', 'source');
  const dependencyRoot = path.join(root, 'hidden', 'deps', 'pkg');
  const siblingRoot = path.join(root, 'hidden', 'unselected');
  const externalExclude = path.join(base, 'external-excludes');
  const write = (relative: string, content: string) => {
    const target = path.join(root, relative);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, content);
    return target;
  };
  const git = (repo: string, args: string[]) => {
    const result = spawnSync('git', ['-C', repo, ...args], {
      encoding: 'utf8', timeout: 5000, maxBuffer: 128 * 1024, windowsHide: true,
      env: { ...process.env, GIT_CONFIG_NOSYSTEM: '1', GIT_TERMINAL_PROMPT: '0' },
    });
    if (result.error || result.status !== 0) {
      throw new Error(`Fixture git ${args.join(' ')} failed: ${result.error ?? result.stderr}`);
    }
    return result.stdout;
  };
  const writeConfig = (includeIgnored = ['hidden/source/', 'hidden/deps/*/']) =>
    write('codegraph.json', JSON.stringify({ includeIgnored, exclude: ['hidden/source/explicit.ts'] }));

  write('.gitignore', '/hidden/\n');
  writeConfig();
  write('ordinary.ts', 'export function unrelatedFact() { return 7; }\n');
  write('hidden/loose.ts', 'export function looseHiddenFact() { return 0; }\n');
  write('hidden/source/entry.ts', "import { sourceLeaf } from './leaf';\nimport { dependencyFact } from '../deps/pkg/dep';\nexport function sourceEntry() { return sourceLeaf() + dependencyFact(); }\n");
  write('hidden/source/leaf.ts', 'export function sourceLeaf() { return 11; }\n');
  write('hidden/source/.gitignore', 'root-ignored.ts\nsealed/\n!sealed/reopened/\n.gitignore\n');
  write('hidden/source/codegraph.json', JSON.stringify({ exclude: ['leaf.ts'] }));
  write('hidden/source/root-ignored.ts', 'export function rootIgnoredFact() {}\n');
  write('hidden/source/explicit.ts', 'export function explicitlyExcludedFact() {}\n');
  write('hidden/source/info-ignored.ts', 'export function infoExcludedFact() {}\n');
  write('hidden/source/external-ignored.ts', 'export function externalExcludedFact() {}\n');
  write('hidden/source/nested/.gitignore', '*.skip.ts\n!allowed.skip.ts\n');
  write('hidden/source/nested/keep.ts', 'export function nestedFact() { return 13; }\n');
  write('hidden/source/nested/ignored.skip.ts', 'export function nestedIgnoredFact() {}\n');
  write('hidden/source/nested/allowed.skip.ts', 'export function negatedFact() { return 17; }\n');
  write('hidden/source/sealed/.gitignore', '!reopened/\n');
  write('hidden/source/sealed/reopened/.gitignore', '!target.ts\n');
  write('hidden/source/sealed/reopened/target.ts', 'export function unreachableFact() {}\n');
  write('hidden/source/dist/forced.ts', 'export function forcedBuildFact() {}\n');
  write('hidden/source/node_modules/forced.ts', 'export function forcedDependencyFact() {}\n');
  write('hidden/source/.codegraph/state.ts', 'export function graphInternalFact() {}\n');
  write('hidden/deps/pkg/dep.ts', 'export function dependencyFact() { return 19; }\n');
  write('hidden/unselected/sibling.ts', 'export function unselectedFact() {}\n');
  for (const repo of [sourceRoot, dependencyRoot, siblingRoot]) git(repo, ['init', '-q']);
  fs.writeFileSync(externalExclude, 'external-ignored.ts\n');
  git(sourceRoot, ['config', '--local', 'core.excludesFile', externalExclude]);
  write('hidden/source/.git/info/exclude', 'info-ignored.ts\n');
  git(sourceRoot, ['add', '-f', 'dist/forced.ts', 'node_modules/forced.ts']);

  return {
    base, root, sourceRoot, dependencyRoot, siblingRoot, externalExclude, write, writeConfig, git,
    /** Literal independent file oracle; no scanner-derived expected values. */
    expectedFiles: [
      'hidden/deps/pkg/dep.ts',
      'hidden/source/entry.ts',
      'hidden/source/leaf.ts',
      'hidden/source/nested/allowed.skip.ts',
      'hidden/source/nested/keep.ts',
      'ordinary.ts',
    ],
    gitIgnored(relative: string): boolean {
      const result = spawnSync('git', ['-C', sourceRoot, 'check-ignore', '--no-index', '-q', '--', relative], {
        encoding: 'utf8', timeout: 5000, maxBuffer: 128 * 1024, windowsHide: true,
      });
      if (result.error || (result.status !== 0 && result.status !== 1)) throw new Error(`git check-ignore failed: ${result.error ?? result.stderr}`);
      return result.status === 0;
    },
    dispose: () => fs.rmSync(base, { recursive: true, force: true }),
  };
}

export type SelectedEmbeddedFixture = ReturnType<typeof createSelectedEmbeddedFixture>;
