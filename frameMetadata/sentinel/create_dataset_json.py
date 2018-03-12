#!/usr/bin/env python
import os, sys, json, re


def get_version():
    """Get dataset version."""

    DS_VERS_CFG = os.path.normpath(
                      os.path.join(
                          os.path.dirname(os.path.abspath(__file__)),
                          '..', '..', 'conf', 'dataset_versions.json'))
    with open(DS_VERS_CFG) as f:
        ds_vers = json.load(f)
    return ds_vers['S1-IW_SLC']


def create_dataset_json(id, met_file, ds_file):
    """Write dataset json."""

    # get metadata
    with open(met_file) as f:
        md = json.load(f)

    # build dataset
    ds = {
        'version': get_version(),
        'label': id,
        'location': {
            'type': 'Polygon',
            'coordinates': [
                [
                    [ md['bbox'][0][1], md['bbox'][0][0] ],
                    [ md['bbox'][1][1], md['bbox'][1][0] ],
                    [ md['bbox'][2][1], md['bbox'][2][0] ],
                    [ md['bbox'][3][1], md['bbox'][3][0] ],
                    [ md['bbox'][0][1], md['bbox'][0][0] ]
                ]
            ]
        },
        'starttime': md['sensingStart'],
        'endtime': md['sensingStop'],
    }

    # write out dataset json
    with open(ds_file, 'w') as f:
        json.dump(ds, f, indent=2)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("usage: %s <id> <metadata json file> <output dataset json file>" % sys.argv[0])
    id = sys.argv[1]
    met_file = sys.argv[2]
    ds_file = sys.argv[3]
    create_dataset_json(id, met_file, ds_file)
