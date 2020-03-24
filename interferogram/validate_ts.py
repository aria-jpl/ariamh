#!/usr/bin/env python3 
from __future__ import absolute_import
from builtins import str
from builtins import map
import os, sys, re, requests, json, shutil, traceback, logging, hashlib, math
from itertools import chain
from subprocess import check_call, CalledProcessError
from glob import glob
from lxml.etree import parse
import numpy as np
from datetime import datetime

from utils.UrlUtils import UrlUtils
from utils.createImage import createImage
from .sentinel.check_interferogram import check_int
from interferogram.stitcher_utils import main as main_st, get_mets, get_dates
from interferogram.validate_ifg import (get_lat_index, get_times, create_dataset_json,
ifg_exists, SetEncoder, group_ifgs, query_hits)



log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('validate_ts')


BASE_PATH = os.path.dirname(__file__)


def get_version():
    """Get dataset version."""

    DS_VERS_CFG = os.path.normpath(
                      os.path.join(
                          os.path.dirname(os.path.abspath(__file__)),
                          '..', 'conf', 'dataset_versions.json'))
    with open(DS_VERS_CFG) as f:
        ds_vers = json.load(f)
    return ds_vers['S1-VALIDATED_TS_STACK']


def create_met_json(id, version, inps, met_json_file):
    """Create HySDS met json file."""

    # build met
    met = {
        'swath': inps['swaths'],
        'direction': inps['direction'],
        'platform': inps['platforms'],
        'trackNumber': inps['track'],
        'archive_filename': inps['output_file'],
        'latitudeIndexMin': inps['latitudeIndexMin'],
        'latitudeIndexMax': inps['latitudeIndexMax'],
        'min_repeat': inps['min_repeat'],
        'max_repeat': inps['max_repeat'],
        'only_best': inps['only_best'],
    }

    # write out dataset json
    with open(met_json_file, 'w') as f:
        json.dump(met, f, indent=2)


def main():
    """HySDS PGE wrapper for Sentinel-1 interferogram validation."""

    # save cwd (working directory)
    cwd = os.getcwd()

    # get context
    ctx_file = os.path.abspath('_context.json')
    if not os.path.exists(ctx_file):
        raise RuntimeError("Failed to find _context.json.")
    with open(ctx_file) as f:
        ctx = json.load(f)
    logger.info("ctx: {}".format(json.dumps(ctx, indent=2, sort_keys=True)))

    # get args
    dataset_tag = ctx['dataset_tag']
    project = ctx['project']
    location = ctx['location']
    min_repeat = int(ctx['min_repeat'])
    max_repeat = int(ctx['max_repeat'])
    only_best = ctx['only_best']
    query = ctx['query']
    conf = ctx.get('conf', 'settings.conf')
    sys_ver = ctx.get('sys_ver', "v1*")
    output_file = 'valid_ts_out.json'
    meta_file = 'valid_meta_ts_out.json'

    # get lat index min/max
    lat_idx_min, lat_idx_max = get_lat_index(location)

    # query hits
    uu = UrlUtils() # url utils obj
    hits = query_hits(uu, query)

    # enumeration of inps
    inps_list = []
    grouped = group_ifgs(hits)
    for track in grouped:
        for direction in grouped[track]:
            gp = grouped[track][direction]
            inps = {
                "swaths": gp['swath'],
                "direction": direction,
                "track": int(track),
                "platforms": gp['platform'],
                "min_repeat": min_repeat,
                "max_repeat": max_repeat,
                "only_best": only_best,
                "mets": gp['mets'],
                "conf": conf,
                "latitudeIndexMin": lat_idx_min,
                "latitudeIndexMax": lat_idx_max,
                "sys_ver": sys_ver,
                "output_file": output_file,
                "meta_file": meta_file,
            }
            inps_list.append(inps)

    #logger.info("inps_list: {}".format(json.dumps(inps_list, indent=2)))

    # create validate_ts output for each input list
    for inps in inps_list:

        # get times
        starttimes, endtimes = get_times(inps['mets'])
        starttime = datetime.strptime(starttimes[0], "%Y-%m-%dT%H:%M:%S%f")
        endtime = datetime.strptime(endtimes[-1], "%Y-%m-%dT%H:%M:%S%f")

        # md5 hash
        md5 = hashlib.md5(json.dumps(inps, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()

        # get id base
        id_base = "S1-VALIDATED_TS_STACK-TN{}_{}-{}_{}-{}_{}_s{}-{}".format(inps['track'],
                                                                            inps['latitudeIndexMin'],
                                                                            inps['latitudeIndexMax'],
                                                                            starttime.strftime("%Y%m%dT%H%M%S"),
                                                                            endtime.strftime("%Y%m%dT%H%M%S"),
                                                                            inps['direction'],
                                                                            "".join(map(str, sorted(inps['swaths']))),
                                                                            md5[0:4])
        
        # get dataset version and set dataset ID
        version = get_version()
        id = "{}-{}-{}".format(id_base, version, re.sub("[^a-zA-Z0-9_]", "_", ctx.get("context", {})
                                                   .get("dataset_tag", "standard")))

        logger.info("Product ID: {}".format(id))

        # get endpoint configurations
        uu = UrlUtils()
        es_url = uu.rest_url
        es_index = "{}_{}_s1-validated_ts_stack".format(uu.grq_index_prefix, version)

        # check if interferogram already exists
        logger.info("GRQ url: {}".format(es_url))
        logger.info("GRQ index: {}".format(es_index))
        logger.info("Product ID for version {}: {}".format(version, id))
        if ifg_exists(es_url, es_index, id):
            logger.info("{} interferogram for {}".format(version, id_base) +
                        " was previously generated and exists in GRQ database.")
            return 0

        # create product directory
        dataset_dir = os.path.abspath(id)
        os.makedirs(dataset_dir, 0o755)

        # chdir
        os.chdir(dataset_dir)

        # run validate_ts
        try:
            mets = inps['mets']
            json.dump(mets,open(inps['meta_file'],'w'), indent=2, sort_keys=True)
            json.dump(inps,open('valid_ts_in.json','w'), indent=2, sort_keys=True)
            main_st(('-a validate_ts_met -i ' + 'valid_ts_in.json').split())

            # create dataset json
            ds_json_file = os.path.join("{}.dataset.json".format(id))
            create_dataset_json(id, version, location, starttime, endtime, ds_json_file)

            # create met json
            met_json_file = os.path.join("{}.met.json".format(id))
            create_met_json(id, version, inps, met_json_file)

        finally:
            # chdir back up to work directory
            os.chdir(cwd)


if __name__ == '__main__':
    try: status = main()
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)
