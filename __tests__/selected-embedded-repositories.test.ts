import { afterEach, describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import { execFileSync } from 'child_process';
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

  // ACL mutation requires the cleanup-enabled Windows wrapper's explicit owned paths.
  it.runIf(process.platform === 'win32' && !!process.env.CODEGRAPH_ACL_FIXTURE_BASE && !!process.env.CODEGRAPH_ACL_RECOVERY_FILE)(
    'rejects actual selected-directory access denial without removing prior indexed facts', async () => {
    if (!path.isAbsolute(process.env.CODEGRAPH_ACL_FIXTURE_BASE!) || !path.isAbsolute(process.env.CODEGRAPH_ACL_RECOVERY_FILE!)) {
      throw new Error('ACL fixture and recovery descriptor paths must be absolute');
    }
    const baseInput = path.resolve(process.env.CODEGRAPH_ACL_FIXTURE_BASE!);
    const descriptor = path.resolve(process.env.CODEGRAPH_ACL_RECOVERY_FILE!);
    if (fs.existsSync(descriptor)) throw new Error('ACL recovery descriptor already exists; reconcile the prior attempt first');
    fixture = createSelectedEmbeddedFixture(baseInput);
    graph = await CodeGraph.init(fixture.root);
    await graph.indexAll();
    expect(graph.getFiles().map((file) => file.path).sort()).toEqual(fixture.expectedFiles);
    const unrelatedId = graph.getNodesByName('unrelatedFact')[0]!.id;
    const entry = graph.getNodesInFile('hidden/source/entry.ts').find((node) => node.name === 'sourceEntry')!;
    const expected = { files: fixture.expectedFiles, unrelatedId, callees: ['dependencyFact', 'sourceLeaf'] };
    const facts = () => ({
      files: graph!.getFiles().map((file) => file.path).sort(),
      unrelatedId: graph!.getNodesByName('unrelatedFact')[0]?.id,
      callees: graph!.getCallees(entry.id).map((result) => result.node.name).sort(),
    });
    expect(facts()).toEqual(expected);

    const target = path.join(fixture.root, 'hidden', 'deps');
    const base = fs.realpathSync(baseInput);
    if (base.toLowerCase() !== baseInput.toLowerCase()) throw new Error('ACL fixture base must be canonical, not redirected');
    const relative = path.relative(base, target);
    if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) throw new Error('ACL target escaped its owned base');
    const descriptorRelative = path.relative(target, descriptor);
    if (!descriptorRelative || (!descriptorRelative.startsWith('..') && !path.isAbsolute(descriptorRelative))) throw new Error('ACL recovery descriptor must be outside the denied directory');
    for (let cursor = target; ; cursor = path.dirname(cursor)) {
      if (fs.lstatSync(cursor).isSymbolicLink()) throw new Error('ACL fixture contains a link');
      if (cursor.toLowerCase() === base.toLowerCase()) break;
    }
    const script = path.join(base, 'restore-fixture-acl.ps1');
    const powershell = path.join(process.env.SystemRoot ?? 'C:\\Windows', 'System32', 'WindowsPowerShell', 'v1.0', 'powershell.exe');
    fs.writeFileSync(script, `param([string]$Mode,[string]$Target,[string]$FixtureBase,[string]$Descriptor)
$ErrorActionPreference = 'Stop'
$cursor = $Target
while ($true) {
  $item = Get-Item -LiteralPath $cursor -Force
  if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'ACL fixture contains a reparse point' }
  if ($cursor -eq $FixtureBase) { break }
  $cursor = [IO.Path]::GetDirectoryName($cursor)
  if (-not $cursor -or (-not $cursor.Equals($FixtureBase,[StringComparison]::OrdinalIgnoreCase) -and -not $cursor.StartsWith($FixtureBase + '\\',[StringComparison]::OrdinalIgnoreCase))) { throw 'ACL target escaped owned fixture' }
}
if ($Mode -eq 'restore') {
  $saved = Get-Content -LiteralPath $Descriptor -Raw | ConvertFrom-Json
  if ($saved.target -ne $Target -or $saved.fixtureBase -ne $FixtureBase) { throw 'ACL recovery target mismatch' }
  $acl = New-Object System.Security.AccessControl.DirectorySecurity
  $acl.SetSecurityDescriptorSddlForm([string]$saved.originalSddl)
  Set-Acl -LiteralPath $Target -AclObject $acl
  $actual = (Get-Acl -LiteralPath $Target).Sddl
  if ($actual -ne $saved.originalSddl) { throw 'Original ACL was not restored exactly' }
  $result = @{target=$Target;fixtureBase=$FixtureBase;state='restored';restoreVerified=$true;restoredSddl=$actual;restoredUtc=[DateTime]::UtcNow.ToString('o')}
  $resultPath = $Descriptor + '.result.json'
  $tempPath = $resultPath + '.' + [Guid]::NewGuid().ToString('N') + '.tmp'
  $bytes = [Text.Encoding]::UTF8.GetBytes(($result | ConvertTo-Json))
  $stream = [IO.File]::Open($tempPath,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
  try { $stream.Write($bytes,0,$bytes.Length); $stream.Flush($true) } finally { $stream.Dispose() }
  if ([IO.File]::Exists($resultPath)) { [IO.File]::Replace($tempPath,$resultPath,$null) } else { [IO.File]::Move($tempPath,$resultPath) }
  exit 0
}
if ($Mode -ne 'deny') { throw 'Unknown fixture ACL operation' }
$original = (Get-Acl -LiteralPath $Target).Sddl
$sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$saved = @{target=$Target;fixtureBase=$FixtureBase;originalSddl=$original;sid=$sid;restoreScript=$PSCommandPath;state='prepared'}
# Recovery input is immutable and durable before the first ACL mutation.
$bytes = [Text.Encoding]::UTF8.GetBytes(($saved | ConvertTo-Json))
$stream = [IO.File]::Open($Descriptor,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
try { $stream.Write($bytes,0,$bytes.Length); $stream.Flush($true) } finally { $stream.Dispose() }
& "$env:SystemRoot\\System32\\icacls.exe" $Target /deny ('*' + $sid + ':(RD)') | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Could not deny ListDirectory on the fixture' }
`);
    const acl = (mode: string) => execFileSync(powershell, [
      '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', script,
      '-Mode', mode, '-Target', target, '-FixtureBase', base, '-Descriptor', descriptor,
    ], { timeout: 10_000, maxBuffer: 128 * 1024, windowsHide: true });
    console.error(`CODEGRAPH_ACL_RECOVERY_FILE=${descriptor}`);
    try {
      acl('deny');
      let denied: NodeJS.ErrnoException | undefined;
      try { fs.readdirSync(target); } catch (error) { denied = error as NodeJS.ErrnoException; }
      expect(['EACCES', 'EPERM'], 'actual Windows ListDirectory denial prerequisite').toContain(denied?.code);
      const childCode = execFileSync(process.execPath, ['-e',
        "try { require('fs').readdirSync(process.argv[1]); process.stdout.write('ALLOWED'); } catch (error) { process.stdout.write(error.code || 'UNKNOWN'); }",
        target,
      ], { encoding: 'utf8', timeout: 5000, maxBuffer: 4096, windowsHide: true });
      expect(['EACCES', 'EPERM']).toContain(childCode);
      let syncError: unknown;
      try { await graph.sync(); } catch (error) { syncError = error; }
      const actual = facts();
      fs.writeFileSync(`${descriptor}.facts.json`, JSON.stringify({
        denial: { mainCode: denied?.code, childCode },
        sync: { rejected: syncError instanceof Error, error: syncError instanceof Error ? syncError.message : null, facts: actual },
      }, null, 2), { flag: 'wx' });
      expect(actual).toEqual(expected);
      expect(syncError, 'inaccessible selected discovery must reject, not complete catch-up').toBeInstanceOf(Error);
    } finally {
      if (fs.existsSync(descriptor)) {
        try { acl('restore'); } catch (error) {
          // Keep the script/fixture intact for the wrapper's bounded outer recovery.
          fixture = undefined;
          throw new Error(`Fixture ACL restoration failed; recovery descriptor: ${descriptor}`, { cause: error });
        }
      }
    }
  });
});
