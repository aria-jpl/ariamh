#!/usr/bin/env python3
"""
Ingest ALOS2 data from a source to a destination:

  1) download data from a source and verify,
  2) extracts data and creates metadata
  3) push data to repository


HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.
"""

import logging, traceback, argparse
import alos2_utils
import alos2_productize

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser( description='Getting ALOS-2 L2.1 / L1.1 data into ARIA')
    parser.add_argument('-d', dest='download_url', type=str, default='',
            help = 'Download url if available')

    return parser.parse_args()

if __name__ == "__main__":
    args = cmdLineParse()
    ctx = alos2_productize.load_context()

    try:
        # first check if we need to read from _context.json
        if not args.download_url:
            # no inputs defined (as per defaults)
            # we need to try to load from context
            args.download_url = ctx["download_url"]

        # TODO: remember to bring back the download
        alos2_utils.download(args.download_url)
        download_source = args.download_url
        alos2_productize.ingest_alos2(download_source)

    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
