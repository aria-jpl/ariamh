#!/usr/bin/env python
import os, sys, json, re, argparse
from lxml.etree import parse


MISSION_RE = re.compile(r'^S1(\w)_')


def create_met_json(ctx_file, slc_json_file, seed_json_file, out_json_file):
    """Write product metadata json."""

    # get context
    with open(ctx_file) as f:
        context = json.load(f)

    # get SLC metadata
    with open(slc_json_file) as f:
        slc_metadata = json.load(f)

    # get metadata extracted from Sentinel1_TOPS.py
    with open(seed_json_file) as f:
        metadata = json.load(f)

    # remove orbit; breaks index into elasticsearch because of it's format
    if 'orbit' in metadata: del metadata['orbit']

    # get mission char
    mis_char = MISSION_RE.search(context.get('file')).group(1)

    metadata['archive_filename'] = context.get('file', None)
    metadata['archive_url'] = context.get('localize_urls', [{ 'url': None}])[0]['url']
    metadata['sensor'] = "SAR-C Sentinel1"
    metadata['platform'] = "Sentinel-1%s" % mis_char
    if 'version' in slc_metadata: metadata['version'] = slc_metadata['version']
    with open(out_json_file, 'w') as f:
        json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract metadata.")
    parser.add_argument("context", help="context json file")
    parser.add_argument("slc", help="SLC metadata json file")
    parser.add_argument("seed", help="seed metadata json file")
    parser.add_argument("output", help="output metadata json file")
    args = parser.parse_args()
    create_met_json(args.context, args.slc, args.seed, args.output)
