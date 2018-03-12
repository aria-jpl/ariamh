#!/usr/bin/env python
import os, sys, json
from subprocess import check_call


def main(context_file, meta_file, input_file):
    """Run vanilla ISCE."""

    # read in context.json
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    with open('context.json') as f:
        context = json.load(f)

    # build input json
    context['metaFile'] = meta_file
    context['productName'] = 'time-series_%s_%s_%s_%s_%s_%s' % (
        context['beamID'], context['direction'], context['latitudeIndexMin'],
        context['latitudeIndexMax'], context['platform'], context['trackNumber'] )

    with open(input_file, 'w') as f:
        json.dump(context, f, indent=2)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3]))
