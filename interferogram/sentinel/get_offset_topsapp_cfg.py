#!/usr/bin/env python
"""
Determine all combinations of topsApp configurations for dense offsets.
"""

import os, sys, re, requests, json, logging, traceback, argparse

from enumerate_topsapp_cfgs import get_topsapp_cfgs as gtc


# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('get_offset_topsapp_cfg')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


DO_ID_TMPL = "S1-DO_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s{}-{}-{}"


def get_topsapp_cfgs(context_file, temporalBaseline=72, id_tmpl=DO_ID_TMPL, minMatch=0, covth=.95):
    """Return all possible topsApp configurations."""
    # get context
    with open(context_file) as f:
        context = json.load(f)

    # get ampcor args
    ampcor_skip_width = int(context['ampcor_skip_width'])
    ampcor_skip_height = int(context['ampcor_skip_height'])
    ampcor_src_win_width = int(context['ampcor_src_win_width'])
    ampcor_src_win_height = int(context['ampcor_src_win_height'])
    ampcor_src_width = int(context['ampcor_src_width'])
    ampcor_src_height = int(context['ampcor_src_height'])
    dem_urls = context['dem_urls']

    # get enumerations
    (projects, stitched_args, auto_bboxes, ifg_ids, master_zip_urls,
    master_orbit_urls, slave_zip_urls, slave_orbit_urls, swathnums,
    bboxes) = gtc(context_file, temporalBaseline=temporalBaseline, id_tmpl=id_tmpl,
                  minMatch=minMatch, covth=covth)

    # return enumerations and ampcor params
    return ( projects, stitched_args, auto_bboxes, ifg_ids, master_zip_urls, 
             master_orbit_urls, slave_zip_urls, slave_orbit_urls, swathnums, 
             bboxes,
             [ampcor_skip_width for i in projects],
             [ampcor_skip_height for i in projects],
             [ampcor_src_win_width for i in projects],
             [ampcor_src_win_height for i in projects],
             [ampcor_src_width for i in projects],
             [ampcor_src_height for i in projects],
             [dem_urls for i in projects]
           )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("context_file", help="context file")
    parser.add_argument("-t", "--temporalBaseline", dest="temporalBaseline",
                        type=int, default=72, help="temporal baseline")
    args = parser.parse_args()
    do_id_dict = {}
    try:
        cfgs = get_topsapp_cfgs_rsp(args.context_file, args.temporalBaseline)
        print("Enumerated %d cfgs:" % len(cfgs[0]))
        for i in range(len(cfgs[0])):
            print("#" * 80)
            print("project: %s" % cfgs[0][i])
            print("stitched: %s" % cfgs[1][i])
            print("auto_bbox: %s" % cfgs[2][i])
            print("ifg_id: %s" % cfgs[3][i])
            print("master_zip_url: %s" % cfgs[4][i])
            print("master_orbit_url: %s" % cfgs[5][i])
            print("slave_zip_url: %s" % cfgs[6][i])
            print("slave_orbit_url: %s" % cfgs[7][i])
            print("swath_nums: %s" % cfgs[8][i])
            print("bbox: %s" % cfgs[9][i])
            print("ampcor_skip_width: %s" % cfgs[10][i])
            print("ampcor_skip_heigth: %s" % cfgs[11][i])
            print("ampcor_src_win_width: %s" % cfgs[12][i])
            print("ampcor_src_win_height: %s" % cfgs[13][i])
            print("ampcor_src_width: %s" % cfgs[14][i])
            print("ampcor_src_height: %s" % cfgs[15][i])
            print("dem_urls: %s" % cfgs[16][i])
            if cfgs[3][i] in do_id_dict: raise RuntimeError("ifg %s already found." % cfgs[3][i])
            do_id_dict[cfgs[3][i]] = True
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("{}\n".format(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("{}\n".format(traceback.format_exc()))
        raise
