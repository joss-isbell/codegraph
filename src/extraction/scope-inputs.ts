import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { createHash } from 'crypto';
import { execFileSync } from 'child_process';

/** The files defining one repository's Git ignore scope, including absent files. */
export interface GitScopeInputs {
  inputPaths: string[];
  signature: string;
}

const cachedInputs = new Map<string, GitScopeInputs>();

function inputSignature(paths: string[]): string {
  const hash = createHash('sha256');
  for (const file of paths) {
    hash.update(file).update('\0');
    try {
      hash.update(fs.readFileSync(file));
    } catch (error) {
      hash.update(`!${(error as NodeJS.ErrnoException).code ?? 'unreadable'}`);
    }
    hash.update('\0');
  }
  return hash.digest('hex');
}

/** Only an unset key (exit 1) or unavailable Git is an expected absence. */
function gitConfig(root: string, args: string[], allowUnset = false): string | null {
  try {
    return execFileSync('git', ['-C', root, 'config', ...args], {
      encoding: 'utf8',
      timeout: 5_000,
      maxBuffer: 5 * 1024 * 1024,
      stdio: ['ignore', 'pipe', 'ignore'],
      windowsHide: true,
    });
  } catch (error) {
    const failure = error as NodeJS.ErrnoException & { status?: number };
    if (failure.code === 'ENOENT' || (allowUnset && failure.status === 1)) return null;
    // A failed config read must not silently remove excludes and authorize a
    // destructive full reconcile against an incomplete membership inventory.
    throw new Error(`Could not read Git ignore configuration for ${root}`, { cause: error });
  }
}

/**
 * Inventory the actual Git inputs that the watcher must observe.
 * Config resolution is reused while its known inputs remain unchanged; normal
 * source events therefore do not spawn Git. Content signatures also notice
 * atomic replacement and edits preserving a file's timestamp/length.
 */
export function readGitScopeInputs(repoRoot: string): GitScopeInputs {
  const root = path.resolve(repoRoot);
  const previous = cachedInputs.get(root);
  if (previous && inputSignature(previous.inputPaths) === previous.signature) {
    return previous;
  }

  const inputs = new Set<string>([path.join(root, '.gitignore')]);
  const add = (file: string): string => {
    const absolute = path.resolve(root, file);
    inputs.add(absolute);
    return absolute;
  };
  const marker = path.join(root, '.git');
  let gitDir: string | null = null;
  try {
    if (fs.statSync(marker).isDirectory()) gitDir = marker;
    else {
      add(marker);
      const pointer = fs.readFileSync(marker, 'utf8').match(/^gitdir:\s*(.+)$/m)?.[1]?.trim();
      if (pointer) gitDir = path.resolve(root, pointer);
    }
  } catch { /* non-Git parent; its selected children are inventoried separately */ }
  if (gitDir) {
    // Match the existing scanner's .git-directory/file resolution policy.
    // Retain absent files so atomic replacement/delete/restore stays observable.
    add(path.join(gitDir, 'config'));
    add(path.join(gitDir, 'info', 'exclude'));
  }
  // Inventory only the effective configured value and its actual origin. Git
  // resolves existing global/includes itself; no speculative config traversal.
  const configured = gitConfig(root, ['--null', '--show-origin', '--get', 'core.excludesFile'], true);
  const [origin, rawValue] = configured?.split('\0') ?? [];
  const value = rawValue?.trim();
  if (origin?.startsWith('file:')) add(origin.slice(5));
  if (value) {
    // Mirror readGitExcludeExtraPatterns, including its current relative-path
    // behavior. This inventory must not introduce new matching semantics.
    const expanded = value === '~' ? os.homedir()
      : value.startsWith('~/') ? path.join(os.homedir(), value.slice(2)) : value;
    add(path.resolve(expanded));
  }
  const inputPaths = [...inputs].sort();
  const result = { inputPaths, signature: inputSignature(inputPaths) };
  cachedInputs.set(root, result);
  return result;
}
