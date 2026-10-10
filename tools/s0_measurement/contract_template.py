"""Export the exact driver contract for inclusion in the parent preregistration."""
import argparse
import json
from pathlib import Path
from interleaved_w0 import driver_contract


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps({'driver_contract': driver_contract()}, indent=2) + '\n')
    print(args.output)


if __name__ == '__main__':
    main()
