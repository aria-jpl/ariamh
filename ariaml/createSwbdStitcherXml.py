#!/usr/bin/env python3
from builtins import str
import os, sys
from utils.UrlUtils import UrlUtils


def createSwbdStitcherXml(outfile):
    """Write stitcher.xml."""

    base_dir = os.path.dirname(__file__)
    tmpl_file = os.path.join(base_dir, 'swbdStitcher.xml.tmpl')
    with open(tmpl_file) as f:
        tmpl = f.read()
    
    uu = UrlUtils()
    with open(outfile, 'w') as f:
        f.write(tmpl.format(wbd_url=uu.wbd_url))


if __name__ == '__main__':
    try: status = createSwbdStitcherXml(sys.argv[1])
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc()) 
        raise
    sys.exit(status)
