#!/usr/bin/env python
import os, sys, json

from hysds.recognize import Recognizer

from utils.UrlUtils import UrlUtils


def add_metadata(product_dir, metadata_file):
    """Add metadata to json file."""

    with open(metadata_file) as f:
        metadata = json.load(f)

    # get datasets config
    uu = UrlUtils()
    dsets_file = uu.datasets_cfg
    r = Recognizer(dsets_file, product_dir, product_dir, 'v0.1')

    # add
    metadata.setdefault('dataset_type', r.getType())
    metadata.setdefault('dataset_level', r.getLevel())

    # overwrite metadata json file
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("%s <product dir> <metadata JSON file>" % sys.argv[0])
        sys.exit(1)

    add_metadata(sys.argv[1], sys.argv[2])
    sys.exit(0)
