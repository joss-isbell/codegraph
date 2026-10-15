"""Run declared query suites under the proved supervisor; require complete JSON results."""
import json
import os
from pathlib import Path
import subprocess
import sys
from events import event, assertion

root = Path(os.environ['BASELINE_ROOT'])
runtime = Path(os.environ['BASELINE_RUNTIME'])
node = runtime / ('node.exe' if os.name == 'nt' else 'bin/node')
out = Path(os.environ['VALIDATION_OUT'])
scenario = os.environ.get('QUERY_TEST_SCENARIO', 'query-tests')
event('start', scenario=scenario)
command = [str(node), '--liftoff-only', str(root / 'scripts/validation/query-tests.mjs'), *sys.argv[1:]]
env = {k: v for k, v in os.environ.items() if k.upper() != 'PATH'}
env['PATH'] = str(runtime) + os.pathsep + os.environ['PATH']
result = subprocess.run(command, cwd=root, env=env)
report = json.loads((out / 'tests.json').read_text()) if (out / 'tests.json').is_file() else {}
tests = [test for suite in report.get('testResults', []) for test in suite.get('assertionResults', [])]
pending = [test['fullName'] for test in tests if test['status'] in ['skipped', 'pending', 'todo']]
expected_skips = json.loads(os.environ.get('QUERY_EXPECTED_SKIPS', '[]'))
declared_files = sorted(str((root / name).resolve()) for name in sys.argv[1:])
returned_files = sorted(str(Path(suite['name']).resolve()) for suite in report.get('testResults', []))
assertion(scenario, 'every declared suite returned a complete report', declared_files == returned_files,
          declared=declared_files, returned=returned_files)
for test in [test for test in tests if test['status'] == 'failed'][:5]:
    print(test['fullName'] + ': ' + '\n'.join(test.get('failureMessages', []))[:1800], file=sys.stderr)
assertion(scenario, 'all declared tests completed successfully', result.returncode == 0 and report.get('success') is True
          and report.get('numTotalTests', 0) > 0 and sorted(pending) == sorted(expected_skips)
          and all(test['status'] in ['passed', 'skipped', 'pending', 'todo'] for test in tests),
          exitCode=result.returncode, total=report.get('numTotalTests'), passedTests=report.get('numPassedTests'),
          failed=report.get('numFailedTests'), pending=pending, expectedSkips=expected_skips)
event('complete', scenario=scenario)
event('final', passed=True, scenarios=[scenario])
