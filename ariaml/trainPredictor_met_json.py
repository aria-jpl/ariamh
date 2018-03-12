#!/usr/bin/env python
import os, sys, json, re
from lxml.etree import parse


def create_met_json(id, ctx_file, json_file):
    """Write product metadata json."""

    # get context
    with open(ctx_file) as f:
        j = json.load(f)

    # set location
    shape = j.get('rule', {}
                 ).get('query', {}
                 ).get('filtered',{}
                 ).get('filter', {}
                 ).get('geo_shape', {}
                 ).get('location', {}
                 ).get('shape', {})
    shape_type = shape.get('type', None)
    if shape_type is None:
        coords = [ [ -179.9, -89.9 ],
                   [ -179.9,  89.9 ],
                   [  179.9,  89.9 ],
                   [  179.9, -89.9 ],
                   [ -179.9, -89.9 ] ]
    elif shape_type == 'envelope':
        coords = [ [ float(shape['coordinates'][0][0]), float(shape['coordinates'][0][1]) ],
                   [ float(shape['coordinates'][0][0]), float(shape['coordinates'][1][1]) ],
                   [ float(shape['coordinates'][1][0]), float(shape['coordinates'][1][1]) ],
                   [ float(shape['coordinates'][1][0]), float(shape['coordinates'][0][1]) ],
                   [ float(shape['coordinates'][0][0]), float(shape['coordinates'][0][1]) ] ]
    else: raise RuntimeError("Shape type '%s' not handled." % shape_type)

    # write metadata
    metadata = {
        "dataset": "predictor_model",
        "data_product_name": id,
        "tags": [ "phunw", "phase unwrapping" ],
        "location": {
            "type": "Polygon",
            "coordinates": [ coords ],
        }
    }
    with open(json_file, 'w') as f:
        json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("usage: %s <product ID> <context file> <output json file>" % sys.argv[0])
    id = sys.argv[1]
    ctx_file = sys.argv[2]
    json_file = sys.argv[3]
    dataset_file = sys.argv[4]
    create_met_json(id, ctx_file, json_file)
    with open(dataset_file, 'w') as f:
        json.dump({"id":id,"version":"v1.0"}, f, indent=2)
