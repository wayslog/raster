"""Offline fixtures only. No result from this suite is native performance evidence."""
import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import interleaved_w0 as bench
import overlay_driver

ROOT = Path(__file__).resolve().parent / 'selftest-output'
ROOT.mkdir(exist_ok=True)


def fixture_row(case, count, role='candidate', elapsed=1000000, p99=100):
    row = bench.expected_result(case, count) | {'engine': 'cpp' if role == 'cpp' else 'rust', 'elapsed_ns': elapsed, 'p99_ns': p99}
    if role != 'cpp':
        row.update(retries=0, pending=0)
    return row


def fixture_records(case='read/uniform/4', count=2000000):
    return [dict(item, case=case, count=count, sample_stride=64,
                 trace_spec_sha256=bench.trace_spec_digest(case, count), status='valid',
                 result=fixture_row(case, count, item['role'])) for item in bench.execution_plan()]


class ProtocolTests(unittest.TestCase):
    def test_closed_form_matches_small_expanded_trace(self):
        for case in bench.CASES:
            operation, distribution, threads = bench.validate_case(case)
            for count in (4, 64, 256, 1024, 16388):
                values = [7] * (threads * 256)
                checksum = 0
                samples = 0
                for worker in range(threads):
                    for index in range(count // threads):
                        key = worker * 256 + index * 17 % 256 if distribution == 'uniform' else worker * 256 if distribution == 'worker-hot' else 0
                        if operation == 'upsert':
                            values[key] = 42
                        elif operation == 'rmw':
                            values[key] += 1
                        checksum = (checksum + values[key]) & bench.MASK64
                        samples += int((index & 63) == ((index // 64) & 63))
                digest = 0xcbf29ce484222325
                for value in values:
                    digest = (digest ^ value) * 0x100000001b3 & bench.MASK64
                expected = bench.expected_result(case, count)
                self.assertEqual((expected['checksum'], expected['digest'], expected['samples'], expected['verified_keys']), (checksum, digest, samples, len(values)))

    def test_exact_long_and_multithread_sample_counts(self):
        self.assertEqual(bench.expected_result('read/uniform/1', 2000000)['samples'], 31250)
        self.assertEqual(bench.expected_result('read/uniform/4', 2000000)['samples'], 31252)
        for threads in (1, 4):
            self.assertEqual(bench.expected_result(f'rmw/shared-hot/{threads}', 400000000)['samples'], 6250000)

    def test_wrong_inputs_outputs_and_duplicate_rows_are_rejected(self):
        case, count = 'read/uniform/4', 2000000
        row = fixture_row(case, count)
        bench.validate_output(json.dumps(row), 'rust', case, count)
        for field, replacement in [('count', 400000000), ('threads', 1), ('distribution', 'shared-hot'),
                                   ('engine', 'cpp'), ('digest', row['digest'] ^ 1), ('samples', 31250),
                                   ('verified_keys', 256), ('pending', -1), ('elapsed_ns', 0)]:
            altered = row | {field: replacement}
            with self.assertRaises(ValueError, msg=field):
                bench.validate_output(json.dumps(altered), 'rust', case, count)
        for output in ('', '{bad json', json.dumps(row) + '\n' + json.dumps(row)):
            with self.assertRaises(ValueError):
                bench.validate_output(output, 'rust', case, count)
        for invalid in ('read/uniform/2', 'delete/uniform/1', 'read/random/1'):
            with self.assertRaises(ValueError):
                bench.validate_case(invalid)

    def test_plan_contains_six_permutations_and_adjacent_cpp(self):
        plan = bench.execution_plan()
        self.assertEqual(len(plan), 28)
        self.assertEqual([row['role'] for row in plan[:4]], ['baseline', 'candidate', 'control', 'cpp'])
        for index, permutation in enumerate(bench.PERMUTATIONS):
            order = [row['role'] for row in plan if row['round'] == index]
            self.assertEqual(tuple(role for role in order if role != 'cpp'), permutation)
            self.assertEqual(order.index('cpp') - order.index('candidate'), -1 if index % 2 == 0 else 1)

    def test_identical_program_fixture_has_unit_ratios(self):
        summary = bench.summarize(fixture_records(), 'read/uniform/4', 2000000)
        self.assertTrue(summary['passed'])
        for comparison in summary['comparisons'].values():
            self.assertEqual(comparison['throughput_ratio'], 1)
            self.assertEqual(comparison['p99_ratio'], 1)

    def test_control_too_fast_or_too_slow_invalidates_numeric_pass(self):
        for control_elapsed in (900000, 1100000):
            records = fixture_records()
            for record in records:
                if record['role'] == 'control':
                    record['result']['elapsed_ns'] = control_elapsed
            summary = bench.summarize(records, 'read/uniform/4', 2000000)
            self.assertFalse(summary['control_valid'])
            self.assertEqual(summary['status'], 'control_invalid')
            candidate = summary['comparisons']['candidate_vs_baseline']
            self.assertTrue(candidate['threshold_passed'])
            self.assertFalse(candidate['passed'])

    def test_missing_reordered_or_failed_round_is_never_selected_away(self):
        records = fixture_records()
        for broken in (records[:-1], [records[1], records[0], *records[2:]], [*records, records[-1]]):
            with self.assertRaises(ValueError):
                bench.summarize(broken, 'read/uniform/4', 2000000)
        records[0]['status'] = 'invalid'
        records[0]['error'] = 'offline invalid warmup fixture'
        summary = bench.summarize(records, 'read/uniform/4', 2000000)
        self.assertFalse(summary['measurement_valid'])
        self.assertIsNone(summary['comparisons'])

    def test_cpu_mask_rejects_shortage_duplicates_and_single_cpu_for_four_workers(self):
        self.assertEqual(bench.choose_affinity([0, 1, 2, 3], 4), [0, 1, 2, 3])
        self.assertEqual(bench.choose_affinity([0, 1, 2, 3], 1), [0])
        for available, workers, requested in [([0, 1], 4, None), ([0, 1, 2, 3], 4, [0]),
                                               ([0, 1, 2, 3], 4, [0, 1, 2, 2]), ([0, 1], 1, [2])]:
            with self.assertRaises(ValueError):
                bench.choose_affinity(available, workers, requested)

    def test_registration_checks_code_parameters_order_thresholds_and_trace_spec(self):
        registration = {'driver_contract': bench.driver_contract()}
        bench.validate_registration(registration, ['read/uniform/4'], 400000000, 600)
        for field, value in [('measurement_script_sha256', '0' * 64), ('allowed_counts', [2000000]),
                             ('execution_plan', []), ('thresholds', {}), ('trace_spec_sha256', {}),
                             ('timeout_seconds', 601)]:
            changed = copy.deepcopy(registration)
            changed['driver_contract'][field] = value
            with self.assertRaises(ValueError, msg=field):
                bench.validate_registration(changed, ['read/uniform/4'], 400000000, 600)

    def test_build_receipt_checks_registration_and_all_binary_identities(self):
        hashes = {'baseline': 'a', 'control': 'a', 'candidate': 'b', 'cpp': 'c'}
        receipt = {'registration_sha256': 'registered', 'binary_sha256': hashes}
        bench.validate_build_receipt(receipt, 'registered', hashes)
        for changed in [receipt | {'registration_sha256': 'wrong'}, receipt | {'binary_sha256': hashes | {'candidate': 'wrong'}}]:
            with self.assertRaises(ValueError):
                bench.validate_build_receipt(changed, 'registered', hashes)

    def test_overlay_is_exact_and_refuses_reapplication_or_other_source(self):
        overlaid = Path(__file__).with_name('parity.rs').read_bytes()
        original = overlaid.replace(overlay_driver.NEW, overlay_driver.OLD)
        output, receipt = overlay_driver.overlay(original)
        self.assertEqual(output, overlaid)
        self.assertEqual(receipt['replacement_count'], 1)
        self.assertEqual(receipt['output_sha256'], '9f6989096f5105c6dcf6b7643af8dd0b50667a553756ebd0bcc24f73da39af7e')
        for broken in (output, original + b'\n'):
            with self.assertRaises(ValueError):
                overlay_driver.overlay(broken)


class ProcessTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='offline-', dir=ROOT)
        self.directory = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_nonzero_process_preserves_both_logs(self):
        prefix = self.directory / 'failure'
        result = bench.run_saved([sys.executable, '-c', 'import sys; print("partial"); print("error",file=sys.stderr); sys.exit(7)'], prefix, 2)
        self.assertEqual(result['returncode'], 7)
        self.assertEqual(prefix.with_suffix('.stdout').read_text().strip(), 'partial')
        self.assertEqual(prefix.with_suffix('.stderr').read_text().strip(), 'error')

    def test_timeout_kills_group_and_preserves_partial_output(self):
        marker = self.directory / 'surviving-descendant.txt'
        child = f'import time; from pathlib import Path; time.sleep(0.6); Path({str(marker)!r}).write_text("survived")'
        command = [sys.executable, '-c', 'import subprocess,sys,time; subprocess.Popen([sys.executable,"-c",' + repr(child) + ']); print("partial-output",flush=True); print("partial-error",file=sys.stderr,flush=True); time.sleep(10)']
        prefix = self.directory / 'timeout'
        result = bench.run_saved(command, prefix, .25)
        self.assertEqual(result['status'], 'timeout')
        self.assertIn('partial-output', prefix.with_suffix('.stdout').read_text())
        self.assertIn('partial-error', prefix.with_suffix('.stderr').read_text())
        time.sleep(.7)
        self.assertFalse(marker.exists(), 'A descendant escaped process-group termination')
        next_result = bench.run_saved([sys.executable, '-c', 'print("next")'], self.directory / 'next', 2)
        self.assertEqual(next_result['status'], 'exited')

    def test_keyboard_interrupt_kills_group_saves_metadata_then_propagates(self):
        process = mock.Mock(pid=123456)
        process.wait.side_effect = [KeyboardInterrupt(), -9]
        process.poll.return_value = None
        prefix = self.directory / 'interrupt'
        with mock.patch.object(bench.subprocess, 'Popen', return_value=process), mock.patch.object(bench.os, 'killpg') as kill:
            with self.assertRaises(KeyboardInterrupt):
                bench.run_saved(['offline-fixture'], prefix, 600)
        kill.assert_called_once_with(123456, signal.SIGKILL)
        self.assertEqual(json.loads(prefix.with_suffix('.process.json').read_text())['status'], 'interrupted')

    def test_failed_process_does_not_skip_remaining_rounds_or_next_case(self):
        binary = self.directory / 'offline-fixture-binary'
        binary.write_bytes(b'Never executed: offline fixture identity only')
        binaries = {role: binary for role in (*bench.ROLES, 'cpp')}
        common = {'purpose': 'Offline fixture; never acceptance evidence',
                  'binary_sha256': {role: bench.digest_bytes(binary.read_bytes()) for role in binaries}}
        mask = {0, 1, 2, 3}
        calls = []
        def set_mask(pid, selected):
            nonlocal mask
            mask = set(selected)
        def fixture_run(command, prefix, timeout):
            calls.append(command)
            native = command[-6:]
            case = '/'.join(native[1:4])
            role = prefix.name.rsplit('-', 1)[1]
            prefix.with_suffix('.stdout').write_text(json.dumps(fixture_row(case, int(native[4]), role)) + '\n')
            prefix.with_suffix('.stderr').write_text('')
            if len(calls) == 1:
                return {'status': 'timeout', 'pid': 42}
            bench.save_json(prefix.with_suffix('.affinity.json'), {'actual_cpu_mask_before_exec': sorted(mask),
                            'expected_cpu_mask': sorted(mask), 'command': native, 'pid': 42})
            return {'status': 'exited', 'pid': 42}
        with mock.patch.object(bench.os, 'sched_setaffinity', side_effect=set_mask, create=True), \
             mock.patch.object(bench.os, 'sched_getaffinity', side_effect=lambda pid: mask, create=True), \
             mock.patch.object(bench, 'run_saved', side_effect=fixture_run):
            first = bench.run_case('read/uniform/1', 2000000, binaries, self.directory, [0, 1, 2, 3], None, 600, common)
            second = bench.run_case('rmw/shared-hot/4', 2000000, binaries, self.directory, [0, 1, 2, 3], None, 600, common)
        self.assertEqual(len(calls), 56)
        self.assertFalse(first['measurement_valid'])
        self.assertTrue(second['passed'])
        self.assertEqual(mask, {0, 1, 2, 3})


if __name__ == '__main__':
    unittest.main(verbosity=2)
