#!/usr/bin/env python
import os, sys, json


def create_prov_json(metadata_file, prov_file):
    """Create product provenance JSON file."""

    with open(metadata_file) as f:
        metadata = json.load(f)

    # extract provenance
    prov_dict = extract_prov(metadata)

    with open(prov_file, 'w') as f:
        json.dump(prov_dict, f, indent=2, sort_keys=True)


def extract_prov(md):
    """Extract provenance info into a dict."""

    prod_prov = {}
    if 'sensingStart' in md:
        prod_prov['acquisition_start_time'] = md['sensingStart'][:25] + 'Z'
    if 'ProductGenerationDate' in md.get('dfdn', {}):
        prod_prov['source_production_time'] = md.get('dfdn', {})['ProductGenerationDate'].replace(' ', 'T')[:25] + 'Z'
    if 'RequestorUserId' in md.get('dfas', {}):
        prod_prov['RequestorUserId'] = md.get('dfas', {})['RequestorUserId']
    if 'bbox' in md:
        coords = [
            [md['bbox'][0][1], md['bbox'][0][0]],
            [md['bbox'][1][1], md['bbox'][1][0]],
            [md['bbox'][3][1], md['bbox'][3][0]],
            [md['bbox'][2][1], md['bbox'][2][0]],
            [md['bbox'][0][1], md['bbox'][0][0]]
        ]
        prod_prov['location'] = {
            'type': 'Polygon',
            'coordinates': [ coords ]
        }

    return prod_prov


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("%s <metadata JSON file> <prov JSON file>" % sys.argv[0])
        sys.exit(1)

    create_prov_json(sys.argv[1], sys.argv[2])
    sys.exit(0)
