"""Apply the single preregistered deadline change to a temporary benchmark driver."""
import argparse
import hashlib
import json
from pathlib import Path

SOURCE_SHA256 = '5d2215505fd94e7717a24abd16cbb1b2af635c7de6a98a0a456e73c489bac4c3'
OLD = b'let limit = Deadline(started + Duration::from_secs(120));'
NEW = b'let limit = Deadline(started + Duration::from_secs(600));'


def overlay(source):
    before = hashlib.sha256(source).hexdigest()
    if before != SOURCE_SHA256:
        raise ValueError('Source driver differs from the frozen f0ccb460 example')
    if source.count(OLD) != 1 or source.count(b'from_secs(120)') != 1:
        raise ValueError('Expected exactly one frozen business deadline')
    output = source.replace(OLD, NEW)
    return output, {
        'source_sha256': before,
        'output_sha256': hashlib.sha256(output).hexdigest(),
        'replacement_count': 1,
        'old': OLD.decode(),
        'new': NEW.decode(),
        'scope': 'Temporary benchmark driver only; runtime source unchanged',
        'timeout_semantics': 'Business Deadline is 600 seconds from worker start. The external 600 second process timeout includes setup and verification, takes precedence, kills the process group and does not guarantee normal close.',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.source.resolve() == args.output.resolve():
        parser.error('Source and temporary output must be different paths')
    output, receipt = overlay(args.source.read_bytes())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(output)
    receipt.update(source=str(args.source.resolve()), output=str(args.output.resolve()))
    print(json.dumps(receipt, indent=2))


if __name__ == '__main__':
    main()
