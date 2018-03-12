#!/usr/bin/env python
import os, sys, json


def create_dataset_json(metadata_file, ds_file):
    """Create dataset JSON file."""

    with open(metadata_file) as f:
        metadata = json.load(f)

    # create dataset json
    ds = {
        'version': 'v0.1',
        'location': {
            'type': 'Polygon',
            'coordinates': [[
                [ metadata['dfdn']['GeoCoordTopLeft'][1], metadata['dfdn']['GeoCoordTopLeft'][0] ],
                [ metadata['dfdn']['GeoCoordTopRight'][1], metadata['dfdn']['GeoCoordTopRight'][0] ],
                [ metadata['dfdn']['GeoCoordBottomRight'][1], metadata['dfdn']['GeoCoordBottomRight'][0] ],
                [ metadata['dfdn']['GeoCoordBottomLeft'][1], metadata['dfdn']['GeoCoordBottomLeft'][0] ],
                [ metadata['dfdn']['GeoCoordTopLeft'][1], metadata['dfdn']['GeoCoordTopLeft'][0] ],
            ]],
        },
        'starttime': metadata['sensingStart'],
        'endtime': metadata['sensingStop'],
    }
    with open(ds_file, 'w') as f:
        json.dump(ds, f, indent=2, sort_keys=True)

    # clean up metadata
    del metadata['orbit']
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("%s <metadata JSON file> <dataset JSON file>" % sys.argv[0])
        sys.exit(1)

    create_dataset_json(sys.argv[1], sys.argv[2])
    sys.exit(0)
