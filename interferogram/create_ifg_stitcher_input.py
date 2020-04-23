#!/usr/bin/env python
"""
Determine all combinations of stitch interferogram configurations"
"""

from builtins import range
import os, sys, json, logging, traceback, argparse


# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('create_ifg_stitcher_input')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


def main(context_file):
    """Create input file."""

    # get context
    with open(context_file) as f:
        context = json.load(f)

    # get args
    project = context['project']
    direction = context.get('direction', 'along')
    extra_products = context.get('extra_products', [])
    filenames = context['filenames']
    id = context['id']
    outname = 'filt_topophase.unw.geo'

    # dump id to text
    with open('id.txt', 'w') as f:
        f.write("{}\n".format(id))

    # create dataset dir
    os.makedirs(id, 0o755)

    # relative paths for filenames
    for i in range(len(filenames)):
        for j in range(len(filenames[i])):
            filenames[i][j] = os.path.join("..", filenames[i][j])

    # dump input file
    inp = {
        'direction': direction,
        'extra_products': extra_products,
        'filenames': filenames,
        'outname': outname,
    }
    ifg_stitch_file = os.path.join(id, "ifg_stitch.json")
    with open(ifg_stitch_file, 'w') as f:
        json.dump(inp, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("context_file", help="context file")
    args = parser.parse_args()
    try:
        main(args.context_file)
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("{}\n".format(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("{}\n".format(traceback.format_exc()))
        raise
