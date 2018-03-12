#!/usr/bin/env python
import os, sys, json, argparse


def write_job_descriptor(json_file, to_set, to_update):
    """Write job descriptor JSON file setting and updating root keys."""

    # read in existing json
    if os.path.exists(json_file):
        with open(json_file) as f: j = json.load(f)
    else: j = {}

    # set values
    if to_set is not None:
        for k, v in to_set: j[k] = json.loads(v)

    # update values
    if to_update is not None:
        for k, v in to_update:
            print(j, k)
            print(j.get(k, None))
            if isinstance(j.get(k, None), dict):
                j[k].update(json.loads(v))
            else:
                j[k] = json.loads(v)

    # write out final json
    with open(json_file, 'w') as f:
        json.dump(j, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Helper util to write or modify job description JSON files.")
    parser.add_argument('--file', required=True, help="job description JSON file")
    parser.add_argument('--set',  dest="to_set", action="append", nargs=2,
                        help="set key to JSON provided")
    parser.add_argument('--update', dest="to_update", action="append", nargs=2,
                        help="update key with JSON provided")
    args = parser.parse_args()

    write_job_descriptor(args.file, args.to_set, args.to_update)
