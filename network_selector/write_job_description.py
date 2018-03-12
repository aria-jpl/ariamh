#!/usr/bin/env python
import os, sys, json, argparse

from jobDescriptorWriter import write_job_descriptor


def write_job_desc(json_file, context_file):
    """Write job descriptor JSON file settings from context."""

    # read in existing json
    if os.path.exists(json_file):
        with open(json_file) as f: j = json.load(f)
    else: j = {}

    # read in context json
    if os.path.exists(context_file):
        with open(context_file) as f: c = json.load(f)
    else:
        raise(RuntimeError("Context file %s doesn't exist." % context_file))

    # fields
    fields = ['project', 'mode', 'workflow', 'unwrapper', 'unwrap',
              'posting', 'filterStrength', 'output_name', 'geolist',
              'productList']

    # loop over fields and write
    for field in fields:
        if field in c:
            if field == 'output_name':
                j.setdefault('networkSelector', {})['outputFile'] = c[field]
            elif field == 'geolist':
                j.setdefault('createInterferogram', {})['geolist'] = c[field].split()
            elif field == 'productList':
                productList = [i for i in c[field].split() if i != "*.geo"]
                if len(productList) > 0:
                    j.setdefault('createInterferogram', {})['productList'] = productList
            else:
                j[field] = c[field]

    # write out final json
    with open(json_file, 'w') as f:
        json.dump(j, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write job description JSON file from context.json.")
    parser.add_argument('--file', required=True, help="job description JSON file")
    parser.add_argument('--context', required=True, help="context JSON file")
    args = parser.parse_args()

    write_job_desc(args.file, args.context)
