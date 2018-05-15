#!/usr/bin/env python
import sys, json

def write_input(ctx_file):
    with open(ctx_file) as f:
        ctx = json.load(f)

    ctx['_triage_additional_globs'] = [ 'S1-IFG*', 'AOI_*', 'celeryconfig.py', 'datasets.json' ]

    with open(ctx_file, 'w') as f:
        json.dump(ctx, f, sort_keys=True, indent=2)


if __name__ == "__main__": write_input(sys.argv[1])
