"""Run frozen W0 traces with six Rust permutations and adjacent C++ pairs."""
import argparse
import datetime
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import signal
import statistics
import subprocess
import sys
import time

MASK64 = (1 << 64) - 1
STRIDE = 64
COUNTS = (2000000, 400000000)
ROLES = ('baseline', 'candidate', 'control')
PERMUTATIONS = tuple(itertools.permutations(ROLES))
CASES = tuple(f'{operation}/{distribution}/{threads}' for operation, distribution, threads in itertools.product(
    ('read', 'upsert', 'rmw'), ('uniform', 'worker-hot', 'shared-hot'), (1, 4)))
THRESHOLDS = {'candidate_min_throughput': .98, 'candidate_max_p99': 1.10,
              'control_throughput_interval': [.98, 1/.98], 'control_p99_interval': [1/1.10, 1.10],
              'cpp_min_throughput': .90, 'cpp_max_p99': 1.10}


def digest_bytes(content):
    return hashlib.sha256(content).hexdigest()


def save_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def validate_case(case):
    if case not in CASES:
        raise ValueError('Case must be read/upsert/rmw, uniform/worker-hot/shared-hot and 1/4 threads')
    operation, distribution, threads = case.split('/')
    return operation, distribution, int(threads)


def expected_result(case, count):
    operation, distribution, threads = validate_case(case)
    if type(count) is not int or count <= 0 or count % threads:
        raise ValueError('Count must be a positive integer divisible by threads')
    per_worker = count // threads
    visits = []
    for key in range(threads * 256):
        if distribution == 'shared-hot':
            number = count if key == 0 else 0
        elif distribution == 'worker-hot':
            number = per_worker if key % 256 == 0 else 0
        else:
            first_index = (key % 256) * 241 % 256
            number = per_worker // 256 + int(first_index < per_worker % 256)
        visits.append(number)
    if sum(visits) != count:
        raise ValueError('Internal oracle visit count mismatch')
    digest = 0xcbf29ce484222325
    for number in visits:
        value = 7 + number if operation == 'rmw' else 42 if operation == 'upsert' and number else 7
        digest = ((digest ^ value) * 0x100000001b3) & MASK64
    checksum = 7 * count if operation == 'read' else 42 * count
    if operation == 'rmw':
        checksum = sum(7 * number + number * (number + 1) // 2 for number in visits)
    full_blocks, trailing = divmod(per_worker, STRIDE)
    samples = threads * (full_blocks + int(full_blocks % STRIDE < trailing))
    return {'operation': operation, 'distribution': distribution, 'threads': threads,
            'count': count, 'checksum': checksum & MASK64, 'digest': digest,
            'verified_keys': threads * 256, 'samples': samples}


def validate_output(output, engine, case, count):
    if engine not in ('rust', 'cpp'):
        raise ValueError('Unknown engine identity')
    objects = []
    for line in output.splitlines():
        if line.lstrip().startswith('{'):
            value = json.loads(line)
            if isinstance(value, dict) and 'engine' in value:
                objects.append(value)
    if len(objects) != 1:
        raise ValueError('Exactly one engine result is required')
    row = objects[0]
    expected = expected_result(case, count) | {'engine': engine}
    for name, value in expected.items():
        if row.get(name) != value or type(row.get(name)) is not type(value):
            raise ValueError(f'Frozen input or oracle mismatch for {name}: got {row.get(name)!r}, expected {value!r}')
    for name in ('elapsed_ns', 'p99_ns'):
        if type(row.get(name)) is not int or row[name] <= 0:
            raise ValueError('Measurement must contain positive integer ' + name)
    if engine == 'rust':
        for name in ('retries', 'pending'):
            if type(row.get(name)) is not int or row[name] < 0:
                raise ValueError('Rust result must contain nonnegative integer ' + name)
    return row


def trace_spec(case, count):
    operation, distribution, threads = validate_case(case)
    return {
        'schema': 'w0_trace_spec_v1',
        'hash_scope': 'Canonical generator, parameters and configuration; not an expanded operation-stream hash',
        'operation': operation, 'distribution': distribution, 'threads': threads,
        'count': count, 'sample_stride': STRIDE, 'refresh_stride': 256,
        'key_bytes': 8, 'value_bytes': 8, 'hash': 'FNV-1a over eight little-endian key bytes',
        'keys_preloaded': threads * 256, 'initial_value': 7, 'upsert_value': 42, 'rmw_delta': 1,
        'key_generator': {'uniform': 'worker*256+(i*17)%256', 'worker-hot': 'worker*256', 'shared-hot': '0'}[distribution],
        'serial_generator': 'i independently for each worker session',
        'sampling_rule': '(i & 63) == ((i // 64) & 63)', 'value_atomic_order': 'SeqCst',
        'configuration': {'index_buckets': 1024, 'page_bytes': 33554432, 'memory_bytes': 268435456,
                          'mutable_fraction': 0.9, 'device': 'NullDevice', 'cache': False,
                          'statistics': False, 'automatic_compaction': False},
        'expected_result': expected_result(case, count),
    }


def trace_spec_digest(case, count):
    return digest_bytes(json.dumps(trace_spec(case, count), sort_keys=True, separators=(',', ':')).encode())


def execution_plan():
    plan = [{'round': -1, 'role': role, 'phase': 'warmup'} for role in ('baseline', 'candidate', 'control', 'cpp')]
    for index, permutation in enumerate(PERMUTATIONS):
        order = list(permutation)
        location = order.index('candidate') + (index % 2)
        order.insert(location, 'cpp')
        plan.extend({'round': index, 'role': role, 'phase': 'measured'} for role in order)
    return plan


def driver_contract():
    directory = Path(__file__).resolve().parent
    return {
        'protocol': 2,
        'measurement_script_sha256': digest_bytes(Path(__file__).read_bytes()),
        'affinity_launcher_sha256': digest_bytes((directory / 'affinity_exec.py').read_bytes()),
        'overlay_script_sha256': digest_bytes((directory / 'overlay_driver.py').read_bytes()),
        'allowed_cases': list(CASES), 'allowed_counts': list(COUNTS),
        'sample_stride': STRIDE, 'timeout_seconds': 600, 'reap_timeout_seconds': 10,
        'execution_plan': execution_plan(), 'thresholds': THRESHOLDS,
        'trace_spec_sha256': {f'{case}@{count}': trace_spec_digest(case, count) for case in CASES for count in COUNTS},
        'driver_overlay': {
            'original_driver_sha256': '5d2215505fd94e7717a24abd16cbb1b2af635c7de6a98a0a456e73c489bac4c3',
            'overlaid_driver_sha256': '9f6989096f5105c6dcf6b7643af8dd0b50667a553756ebd0bcc24f73da39af7e',
            'business_deadline_seconds': 600, 'replacement_count': 1,
        },
        'affinity_policy': 'Exactly one allowed logical CPU per requested worker; same mask for all four roles; child records the actual inherited mask before same-PID exec.',
        'control_policy': 'Control aliases the exact baseline executable path and SHA256.',
        'failure_policy': 'Keep and execute the complete planned grid after isolated failures; never rerun, select a subset, or compute ratios if any scheduled process is invalid.',
    }


def validate_registration(registration, cases, count, timeout):
    contract = registration.get('driver_contract')
    expected = driver_contract()
    if contract != expected:
        if not isinstance(contract, dict):
            raise ValueError('Registration is missing the complete driver_contract')
        differences = sorted(key for key in set(contract) | set(expected) if contract.get(key) != expected.get(key))
        raise ValueError('Pre-measurement driver contract mismatch: ' + ', '.join(differences))
    if any(case not in contract['allowed_cases'] for case in cases) or count not in contract['allowed_counts'] or timeout != contract['timeout_seconds']:
        raise ValueError('Requested measurement parameters differ from registration')


def validate_build_receipt(receipt, registration_sha256, binary_hashes):
    if receipt.get('registration_sha256') != registration_sha256:
        raise ValueError('Build receipt is not bound to the selected preregistration')
    if receipt.get('binary_sha256') != binary_hashes:
        raise ValueError('Actual binary identities differ from the frozen build receipt')
    if binary_hashes['baseline'] != binary_hashes['control']:
        raise ValueError('Control executable hash differs from baseline')


def choose_affinity(available, threads, requested=None):
    if threads not in (1, 4):
        raise ValueError('Unsupported thread count')
    available = sorted(set(available))
    selected = available[:threads] if requested is None else list(requested)
    if len(selected) != threads or len(set(selected)) != threads:
        raise ValueError('CPU mask must contain exactly one allowed CPU per requested worker')
    if any(type(cpu) is not int or cpu < 0 or cpu not in available for cpu in selected):
        raise ValueError('CPU mask contains an unavailable CPU')
    return sorted(selected)


class UnreapedProcessError(RuntimeError):
    pass


def run_saved(command, prefix, timeout):
    """Save partial output even if the complete new process group times out."""
    save_json(prefix.with_suffix('.command.json'), command)
    started = time.monotonic_ns()
    metadata = {'command': command, 'timeout_seconds': timeout,
                'command_sha256': digest_bytes(json.dumps(command, separators=(',', ':')).encode()),
                'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
    process = None
    interrupted = None
    with prefix.with_suffix('.stdout').open('wb') as output, prefix.with_suffix('.stderr').open('wb') as errors:
        try:
            process = subprocess.Popen(command, stdout=output, stderr=errors, start_new_session=True)
            metadata['pid'] = process.pid
            metadata['process_group'] = process.pid
            try:
                metadata['returncode'] = process.wait(timeout=timeout)
                metadata['status'] = 'exited' if process.returncode == 0 else 'nonzero_exit'
            except subprocess.TimeoutExpired:
                metadata['status'] = 'timeout'
        except OSError as error:
            metadata.update(status='spawn_error' if process is None else 'process_control_error',
                            error=str(error), returncode=None)
        except BaseException as error:
            interrupted = error
            metadata.update(status='interrupted', error=type(error).__name__ + ': ' + str(error))
        finally:
            if process is not None and (process.poll() is None or metadata.get('status') in ('timeout', 'interrupted', 'process_control_error')):
                metadata['termination'] = 'SIGKILL sent to the entire new process group'
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    metadata['termination'] = 'Process group exited before SIGKILL delivery'
                try:
                    metadata['returncode'] = process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    metadata['status'] = 'unreaped_after_sigkill'
    metadata['process_wall_ns'] = time.monotonic_ns() - started
    metadata['stdout_sha256'] = digest_bytes(prefix.with_suffix('.stdout').read_bytes())
    metadata['stderr_sha256'] = digest_bytes(prefix.with_suffix('.stderr').read_bytes())
    save_json(prefix.with_suffix('.process.json'), metadata)
    if metadata['status'] == 'unreaped_after_sigkill':
        raise UnreapedProcessError('Cannot guarantee an isolated next sample while the timed-out process remains unreaped')
    if interrupted is not None:
        raise interrupted
    return metadata


def summarize(records, case, count):
    plan = execution_plan()
    identities = lambda rows: [(row['round'], row['role'], row['phase']) for row in rows]
    if identities(records) != identities(plan):
        raise ValueError('Missing, duplicated, or reordered scheduled process')
    if any(row.get('case') != case or row.get('count') != count or row.get('sample_stride') != STRIDE for row in records):
        raise ValueError('Record case, count or stride differs from the scheduled trace')
    if any(row.get('trace_spec_sha256') != trace_spec_digest(case, count) for row in records):
        raise ValueError('Record trace specification differs across roles or from the registered case')
    failures = [{'round': row['round'], 'role': row['role'], 'status': row['status'], 'error': row.get('error')}
                for row in records if row['status'] != 'valid']
    if failures:
        return {'case': case, 'count': count, 'measurement_valid': False, 'control_valid': False,
                'status': 'measurement_failed', 'failures': failures,
                'comparisons': None, 'passed': False,
                'reason': 'Incomplete or invalid processes are never dropped to calculate a selected subset'}
    rows = {role: [] for role in (*ROLES, 'cpp')}
    for entry in records:
        expected_engine = 'cpp' if entry['role'] == 'cpp' else 'rust'
        result = validate_output(json.dumps(entry['result']), expected_engine, case, count)
        if entry['phase'] == 'measured':
            rows[entry['role']].append(result)
    median_time = {role: statistics.median(row['elapsed_ns'] for row in values) for role, values in rows.items()}
    median_p99 = {role: statistics.median(row['p99_ns'] for row in values) for role, values in rows.items()}
    comparisons = {}
    pairs = [('candidate_vs_baseline', 'candidate', 'baseline', .98),
             ('control_vs_baseline', 'control', 'baseline', .98),
             ('candidate_vs_cpp', 'candidate', 'cpp', .90)]
    for name, numerator, denominator, minimum in pairs:
        throughput = median_time[denominator] / median_time[numerator]
        p99 = median_p99[numerator] / median_p99[denominator]
        passed = throughput >= minimum and p99 <= 1.10
        if name == 'control_vs_baseline':
            passed = .98 <= throughput <= 1 / .98 and 1 / 1.10 <= p99 <= 1.10
        comparisons[name] = {'throughput_ratio': throughput, 'p99_ratio': p99, 'passed': passed}
    control_valid = comparisons['control_vs_baseline']['passed']
    for comparison in comparisons.values():
        comparison['threshold_passed'] = comparison['passed']
        comparison['evidence_valid'] = control_valid
        comparison['passed'] = comparison['threshold_passed'] and control_valid
    per_round = []
    for index in range(6):
        values = {role: rows[role][index] for role in rows}
        ratios = {}
        for name, numerator, denominator, minimum in pairs:
            ratios[name] = {'throughput_ratio': values[denominator]['elapsed_ns'] / values[numerator]['elapsed_ns'],
                            'p99_ratio': values[numerator]['p99_ns'] / values[denominator]['p99_ns']}
        per_round.append({'round': index, 'comparisons': ratios})
    return {'case': case, 'count': count, 'measurement_valid': True,
            'control_valid': control_valid, 'status': 'complete' if control_valid else 'control_invalid',
            'comparisons': comparisons, 'ratios_by_round': per_round,
            'passed': control_valid and all(row['passed'] for row in comparisons.values()),
            'median_elapsed_ns': median_time, 'median_p99_ns': median_p99,
            'scope': 'One case and count on one runner; an invalid control forbids accepting any per-case performance delta'}


def read_host_details():
    details = {'platform': platform.platform(), 'python': sys.version, 'logical_cpu_count': os.cpu_count()}
    for name, command in [('lscpu', ['lscpu']), ('uname', ['uname', '-a'])]:
        try:
            completed = subprocess.run(command, capture_output=True, text=True, timeout=10)
            details[name] = {'returncode': completed.returncode, 'stdout': completed.stdout, 'stderr': completed.stderr}
        except (OSError, subprocess.TimeoutExpired) as error:
            details[name] = {'error': str(error)}
    for name in ('RUNNER_NAME', 'RUNNER_OS', 'RUNNER_ARCH', 'ImageOS', 'ImageVersion', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_JOB'):
        if name in os.environ:
            details[name] = os.environ[name]
    return details


def run_case(case, count, binaries, root, available, requested, timeout, common):
    operation, distribution, threads = validate_case(case)
    selected = choose_affinity(available, threads, requested)
    directory = root / case.replace('/', '-')
    directory.mkdir(exist_ok=False)
    environment = dict(common, case=case, count=count, sample_stride=STRIDE,
                       available_cpus=available, selected_cpu_mask=selected,
                       worker_threads=threads, execution_plan=execution_plan(), timeout_seconds=timeout,
                       trace_spec_sha256=trace_spec_digest(case, count))
    save_json(directory / 'trace_spec.json', trace_spec(case, count))
    save_json(directory / 'environment.json', environment)
    records = []
    launcher = Path(__file__).resolve().with_name('affinity_exec.py')
    try:
        os.sched_setaffinity(0, set(selected))
        for scheduled in execution_plan():
            role = scheduled['role']
            prefix = directory / f"round-{scheduled['round']}-{role}"
            entry = dict(scheduled, case=case, count=count, sample_stride=STRIDE,
                         expected_binary_sha256=common['binary_sha256'][role],
                         trace_spec_sha256=trace_spec_digest(case, count))
            if sorted(os.sched_getaffinity(0)) != selected:
                entry.update(status='affinity_error', error='Parent CPU mask changed before spawn')
                records.append(entry)
                save_json(directory / 'records.json', records)
                continue
            engine_command = [str(binaries[role]), operation, distribution, str(threads), str(count), str(STRIDE)]
            save_json(prefix.with_suffix('.engine-command.json'), engine_command)
            child_evidence = prefix.with_suffix('.affinity.json')
            command = [sys.executable, str(launcher), '--cpus', ','.join(map(str, selected)), '--evidence', str(child_evidence), '--', *engine_command]
            result = run_saved(command, prefix, timeout)
            entry['process'] = result
            try:
                if result['status'] != 'exited':
                    raise ValueError('Native process did not complete normally: ' + result['status'])
                child = json.loads(child_evidence.read_text())
                if (child['actual_cpu_mask_before_exec'] != selected or child['expected_cpu_mask'] != selected
                        or child['command'] != engine_command or child['pid'] != result['pid']):
                    raise ValueError('Child CPU mask, PID or command differs from the frozen sample')
                entry['child_affinity'] = child
                entry['result'] = validate_output(prefix.with_suffix('.stdout').read_text(errors='replace'),
                                                  'cpp' if role == 'cpp' else 'rust', case, count)
                entry['status'] = 'valid'
            except (ValueError, OSError, KeyError, TypeError) as error:
                entry.update(status='invalid', error=type(error).__name__ + ': ' + str(error))
            records.append(entry)
            save_json(directory / 'records.json', records)
            print(json.dumps({'case': case, 'count': count, 'round': entry['round'], 'role': role,
                              'status': entry['status'], 'error': entry.get('error')}), flush=True)
    finally:
        os.sched_setaffinity(0, set(available))
    current_hashes = {role: digest_bytes(binary.read_bytes()) for role, binary in binaries.items()}
    if current_hashes != common['binary_sha256']:
        summary = {'case': case, 'count': count, 'measurement_valid': False, 'passed': False,
                   'status': 'binary_changed', 'observed_hashes_after': current_hashes}
    else:
        summary = summarize(records, case, count)
    save_json(directory / 'comparison.json', summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--cpp', type=Path, required=True)
    parser.add_argument('--case')
    parser.add_argument('--count', type=int, choices=COUNTS, required=True)
    parser.add_argument('--timeout', type=int, choices=(600,), default=600)
    parser.add_argument('--cpus', help='Comma-separated CPUs; exactly one CPU per worker')
    parser.add_argument('--registration', type=Path, required=True)
    parser.add_argument('--registration-sha256', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--build-receipt-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if platform.system() != 'Linux' or not hasattr(os, 'sched_setaffinity'):
        parser.error('Acceptance execution requires native Linux CPU affinity; offline tests use separate fixtures')
    cases = (args.case,) if args.case else CASES
    try:
        for case in cases:
            validate_case(case)
        requested = None if args.cpus is None else [int(value) for value in args.cpus.split(',')]
        available = sorted(os.sched_getaffinity(0))
        for case in cases:
            choose_affinity(available, validate_case(case)[2], requested)
        registration = args.registration.read_bytes()
        if digest_bytes(registration) != args.registration_sha256:
            raise ValueError('Registration digest differs from the supplied frozen identity')
        registration_object = json.loads(registration)
        if not isinstance(registration_object, dict):
            raise ValueError('Registration must be a JSON object')
        validate_registration(registration_object, cases, args.count, args.timeout)
        receipt_bytes = args.build_receipt.read_bytes()
        if digest_bytes(receipt_bytes) != args.build_receipt_sha256:
            raise ValueError('Build receipt digest differs from its frozen identity')
        receipt = json.loads(receipt_bytes)
        if not isinstance(receipt, dict):
            raise ValueError('Build receipt must be a JSON object')
    except (ValueError, OSError) as error:
        parser.error(str(error))
    binaries = {'baseline': args.baseline.resolve(), 'candidate': args.candidate.resolve(),
                'control': args.baseline.resolve(), 'cpp': args.cpp.resolve()}
    if any(not path.is_file() or not os.access(path, os.X_OK) for path in binaries.values()):
        parser.error('Every native binary must exist and be executable')
    binary_hashes = {role: digest_bytes(path.read_bytes()) for role, path in binaries.items()}
    try:
        validate_build_receipt(receipt, args.registration_sha256, binary_hashes)
    except ValueError as error:
        parser.error(str(error))
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / 'registration.json').write_bytes(registration)
    (args.output / 'build-receipt.json').write_bytes(receipt_bytes)
    common = {
        'protocol': 2, 'purpose': 'Native W0 acceptance timing; never synthetic self-test evidence',
        'registration_sha256': args.registration_sha256,
        'build_receipt_sha256': args.build_receipt_sha256,
        'created_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'host': read_host_details(), 'binaries': {role: str(path) for role, path in binaries.items()},
        'binary_sha256': binary_hashes,
        'measurement_script_sha256': digest_bytes(Path(__file__).read_bytes()),
        'affinity_launcher_sha256': digest_bytes(Path(__file__).with_name('affinity_exec.py').read_bytes()),
        'thresholds': THRESHOLDS,
        'timeout_scope': '600 second external wall limit includes launcher, engine setup, timed business and full final verification. It overrides the temporary 600 second business Deadline and may kill without normal close.',
        'failure_policy': 'Keep every scheduled warmup and measurement, continue after isolated failures, never rerun or select a valid subset. An unreaped process aborts this driver because isolation is no longer assured.',
    }
    if common['binary_sha256']['baseline'] != common['binary_sha256']['control']:
        raise RuntimeError('Control binary identity must equal baseline')
    save_json(args.output / 'environment.json', common)
    summaries = []
    for case in cases:
        try:
            summary = run_case(case, args.count, binaries, args.output, available, requested, args.timeout, common)
        except UnreapedProcessError as error:
            summaries.append({'case': case, 'count': args.count, 'passed': False,
                              'status': 'isolation_lost', 'error': str(error)})
            save_json(args.output / 'comparison.json', summaries)
            raise
        except Exception as error:
            summary = {'case': case, 'count': args.count, 'passed': False,
                       'status': 'case_error', 'error': type(error).__name__ + ': ' + str(error)}
        summaries.append(summary)
        save_json(args.output / 'comparison.json', summaries)
        print(json.dumps(summary), flush=True)
    raise SystemExit(0 if all(row['passed'] for row in summaries) else 1)


if __name__ == '__main__':
    main()
