"""Record the actual child CPU mask, then replace this process with the engine."""
import argparse
import datetime
import json
import os
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cpus', required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command and command[0] == '--':
        command = command[1:]
    if not command or not hasattr(os, 'sched_getaffinity'):
        parser.error('Native execution requires a command and Linux CPU affinity')
    wanted = sorted(int(value) for value in args.cpus.split(','))
    actual = sorted(os.sched_getaffinity(0))
    evidence = {
        'pid': os.getpid(), 'parent_pid': os.getppid(),
        'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'expected_cpu_mask': wanted, 'actual_cpu_mask_before_exec': actual,
        'command': command,
        'method': 'This same PID replaces itself with the native binary; subsequently created worker threads inherit its mask.',
    }
    args.evidence.write_text(json.dumps(evidence, indent=2) + '\n')
    if actual != wanted or len(wanted) != len(set(wanted)):
        print('Child CPU mask differs from the registered case mask', file=sys.stderr)
        raise SystemExit(2)
    os.execv(command[0], command)


if __name__ == '__main__':
    main()
