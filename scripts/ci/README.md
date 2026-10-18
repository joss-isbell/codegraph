# Ordinary Windows CI

The independent `helper-tests` job runs the existing native-cleanup regression
and six receipt-consumer regressions without npm, a product build, or the product
test workspace. Run that same bounded check locally on Windows with Python 3.9
or newer:

```powershell
python -B scripts/ci/selftest.py
```

It runs `windows.test.py` then `verify-windows.test.py` once each. Both must
complete successfully; a passing prefix, interruption, timeout or incomplete
cleanup fails the command. It reuses the ordinary CI supervisor, with 120 seconds
per suite and 360 seconds overall, including the existing 70-second final cleanup
reserve. These are supervision limits, not duration promises. Fresh helper logs
and receipts go under `RUNNER_TEMP/codegraph-windows-ci-helpers` (or
`.validation/windows-ci-helpers` locally), separate from product CI receipts.
The helper job always attempts to upload its bounded logs and receipts, retained
for seven days. Neither Windows job depends on the other passing.

`.github/workflows/windows-ci.yml` runs the existing `npm ci`, `npm run build`,
and `npm test` commands on pull requests and pushes to `main` or `proto/issue-7`.
The test command covers both Vitest workspace projects; the separate component
library packaging build is not part of this check.

Run the same sequence locally on Windows with supported Node/npm and Python
3.9 or newer on `PATH`:

```powershell
python scripts/ci/windows.py
```

After the command exits, consume the complete receipt (the hosted workflow does
this automatically):

```powershell
python scripts/ci/verify-windows.py "$env:RUNNER_TEMP/codegraph-windows-ci"
```

For local runs without `RUNNER_TEMP`, use `.validation/windows-ci`. Use a fresh
output base for each check: the consumer requires exactly one attempt, rejects
incomplete install/build/test sequences, and treats unreadable paths or invalid
receipt files as failures, never as absent attempts. Keep the command's original
nonzero exit even if receipt verification also fails. Local receipt verification
does not certify source integrity or replace the actual command exit status.

All three commands use `CODEGRAPH_DIR=.codegraph` so an inherited local agent
index-directory override cannot change the repository's canonical CI layout.

The adapter reuses the validation runner's Windows Job containment and bounded
log drains. It runs each command once, stops at the first failure, and treats
timeouts, overflow, nonzero exits, and incomplete or forced cleanup as failures.
Command budgets are 10 minutes for installation, 10 for the build, and 30 for
tests, within a 55-minute overall budget. These are CI supervision bounds, not
product request deadlines; they do not lower any test's own timeout.

Each attempt has a fresh directory under `RUNNER_TEMP/codegraph-windows-ci`
(or `.validation/windows-ci` locally), with per-command receipts and up to
10 MiB per output stream. A failed command prints an 8 KiB tail and its log
location. Hosted runs upload only that attempt output, retained for seven days;
a canceled or lost runner may not complete its upload. The attempt-local npm
cache is excluded from the artifact.

This is an ordinary product build/test check, not Issue #7's private acceptance
campaign or a source-integrity qualification. It does not invoke the validation
runner's scenario consumer or file-change Witness. Adding either job does not
make it a required branch-protection check, and local validation is not a hosted
GitHub Actions run.

A helper-test pass only checks the CI helpers. It does not establish a product
install/build/test pass; those commands still need their own complete receipt.
