#!/usr/bin/env python
import sys, json

def write_input(ctx_file, in_file):
    with open(ctx_file) as f:
        j = json.load(f)

    input = { "url": j['rule_hit']['_source']['urls'][0] }
    with open(in_file, 'w') as f:
        json.dump(input, f, indent=2)


if __name__ == "__main__": write_input(sys.argv[1], sys.argv[2])
