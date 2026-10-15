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
pending = [test['fullName'] for suite in report.get('testResults', []) for test in suite.get('assertionResults', []) if test['status'] in ['pending', 'todo']]
expected_skips = json.loads(os.environ.get('QUERY_EXPECTED_SKIPS', '[]'))
assertion(scenario, 'all declared tests completed successfully', result.returncode == 0 and report.get('success') is True
          and report.get('numTotalTests', 0) > 0 and sorted(pending) == sorted(expected_skips),
          exitCode=result.returncode, total=report.get('numTotalTests'), passedTests=report.get('numPassedTests'),
          failed=report.get('numFailedTests'), pending=pending, expectedSkips=expected_skips)
event('complete', scenario=scenario)
event('final', passed=True, scenarios=[scenario])
