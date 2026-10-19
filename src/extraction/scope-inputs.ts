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

/**
 * Resolve the repository GIT_DIR for `repoRoot` (a `.git` directory, or the
 * target of a `.git` file pointer). Null when this isn't a git checkout.
 */
export function resolveGitDir(repoRoot: string): string | null {
  const gitPath = path.join(repoRoot, '.git');
  let st: fs.Stats;
  try {
    st = fs.statSync(gitPath);
  } catch {
    return null;
  }
  if (st.isDirectory()) return gitPath;
  if (!st.isFile()) return null;
  try {
    const raw = fs.readFileSync(gitPath, 'utf8').match(/^gitdir:\s*(.+)$/m)?.[1]?.trim();
    if (!raw) return null;
    return path.isAbsolute(raw) ? path.normalize(raw) : path.resolve(repoRoot, raw);
  } catch {
    return null;
  }
}

/** Expand a leading `~/` the way git does for `core.excludesFile`. */
export function expandUserPath(p: string): string {
  if (p === '~') return os.homedir();
  if (p.startsWith('~/')) return path.join(os.homedir(), p.slice(2));
  return p;
}

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
  const gitDir = resolveGitDir(root);
  try {
    if (fs.statSync(marker).isFile()) add(marker);
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
    add(path.resolve(expandUserPath(value)));
  }
  const inputPaths = [...inputs].sort();
  const result = { inputPaths, signature: inputSignature(inputPaths) };
  cachedInputs.set(root, result);
  return result;
}
