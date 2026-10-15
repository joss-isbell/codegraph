"""Prove the real Vitest adapter accepts empty errors and rejects an unhandled rejection."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from events import event, assertion
from report import consume, digest
from runner import write

root = Path(os.environ['BASELINE_ROOT'])
out = Path(os.environ['VALIDATION_OUT'])
runtime = Path(os.environ['BASELINE_RUNTIME'])
scenario = 'query-adapter-proof'
event('start', scenario=scenario)

summaries = []
for name, rejection in [('clean', False), ('unhandled', True)]:
    fixture = out / (name + '-fixture')
    adapter_dir = fixture / 'scripts/validation'
    test_dir = fixture / '__tests__'
    adapter_dir.mkdir(parents=True)
    test_dir.mkdir()
    for file in ['query-tests.mjs', 'query-workspace.mjs']:
        shutil.copyfile(root / 'scripts/validation' / file, adapter_dir / file)
        assertion(scenario, 'synthetic fixture uses the exact outer-bound actual adapter bytes',
                  digest(root / 'scripts/validation' / file) == digest(adapter_dir / file), case=name, file=file)
    shutil.copyfile(root / '__tests__/setup-home-sandbox.ts', test_dir / 'setup-home-sandbox.ts')
    # This deliberately synthetic, non-Git source root binds every fixture byte.
    # The outer runner separately binds the actual product/adapter Git snapshot.
    (fixture / 'baseline-source-commit.txt').write_text(os.environ['BASELINE_SOURCE_COMMIT'] + '\n', encoding='utf-8')
    # The single assertion passes in both cases. Only the real Vitest unhandled
    # rejection channel distinguishes the second producer/consumer chain.
    test = "import { it, expect } from 'vitest';\nit('adapter proof assertion', async () => {\n  expect(2 + 2).toBe(4);\n"
    if rejection:
        test += "  Promise.reject(new Error('ticket-13-unhandled-proof'));\n  await new Promise(resolve => setTimeout(resolve, 100));\n"
    test += "});\n"
    (test_dir / 'adapter.test.ts').write_text(test, encoding='utf-8')
    attempt = out / (name + '-attempt')
    plan = {
        'output': str(attempt), 'cwd': str(fixture), 'sourceRoot': str(fixture),
        'deadlineSeconds': 90, 'scenarios': ['query-tests'],
        'command': [sys.executable, str(root / 'scripts/validation/query-tests.py'), '__tests__/adapter.test.ts'],
        'environment': {'BASELINE_ROOT': str(fixture), 'BASELINE_RUNTIME': str(runtime), 'QUERY_EXPECTED_SKIPS': '[]'},
        'artifacts': [str(runtime), str(root / 'node_modules'), str(fixture)],
    }
    plan_path = out / (name + '.plan.json')
    write(plan_path, plan)
    completed = subprocess.run([sys.executable, str(root / 'scripts/validation/runner.py'), str(plan_path)], timeout=210)
    result = consume(attempt / 'receipt.json')
    receipt = json.loads((attempt / 'receipt.json').read_text())
    tests = json.loads((attempt / 'tests.json').read_text())
    diagnostics = json.loads((attempt / 'query-diagnostics.json').read_text())
    checkpoints = diagnostics['checkpoints']
    assertion(scenario, 'fixture assertion passes in both actual Vitest runs', tests['success'] is True
              and tests['numTotalTests'] == 1 and tests['numPassedTests'] == 1 and tests['numFailedTests'] == 0, case=name)
    assertion(scenario, 'complete actual adapter diagnostic collection shape was accepted',
              any(row.get('name') == 'Vitest error diagnostics have the complete declared shape'
                  and row.get('passed') is True for row in receipt['events']), case=name)
    if rejection:
        assertion(scenario, 'unhandled rejection retains identity and fails the actual consumer',
                  completed.returncode != 0 and result['outcome'] == 'fail'
                  and any(row.get('name') == 'Vitest reported no unhandled errors through process exit'
                          and row.get('passed') is False for row in receipt['events'])
                  and any(error.get('message') == 'ticket-13-unhandled-proof' and 'adapter.test.ts' in (error.get('stack') or '')
                          for checkpoint in checkpoints for error in checkpoint['errors'])
                  and checkpoints[-1]['phase'] == 'exit' and checkpoints[-1]['exitCode'] == 1, case=name, summary=result)
    else:
        assertion(scenario, 'empty unhandled collection and successful exit pass the actual consumer',
                  completed.returncode == 0 and result['outcome'] == 'pass'
                  and all(checkpoint['errorCount'] == 0 and checkpoint['errors'] == [] and checkpoint['exitCode'] == 0
                          for checkpoint in checkpoints), case=name, summary=result)
    assertion(scenario, 'both diagnostic attempts keep stable inputs and complete owned cleanup',
              receipt['bindingStable'] is True and receipt['cleanup']['complete'] is True, case=name)
    summaries.append({'case': name, 'summary': result, 'diagnostics': diagnostics})

write(out / 'adapter-proof.json', summaries)
event('complete', scenario=scenario)
event('final', passed=True, scenarios=[scenario])
