#!/usr/bin/env python3 
from __future__ import absolute_import
from builtins import str
from builtins import map
import os, sys, re, requests, json, shutil, traceback, logging, hashlib, math
from bisect import insort
from itertools import chain
from subprocess import check_call, CalledProcessError
from glob import glob
from lxml.etree import parse
import numpy as np
from datetime import datetime
from osgeo import ogr, osr

from utils.UrlUtils import UrlUtils
from utils.createImage import createImage
from .sentinel.check_interferogram import check_int
from interferogram.stitcher_utils import main as main_st, get_mets, get_dates


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('validate_ifg')


BASE_PATH = os.path.dirname(__file__)


def get_version():
    """Get dataset version."""

    DS_VERS_CFG = os.path.normpath(
                      os.path.join(
                          os.path.dirname(os.path.abspath(__file__)),
                          '..', 'conf', 'dataset_versions.json'))
    with open(DS_VERS_CFG) as f:
        ds_vers = json.load(f)
    return ds_vers['S1-VALIDATED_IFG_STACK']


def get_lat_index(location):
    """Get latitude index min and max."""

    geom = ogr.CreateGeometryFromJson(json.dumps(location))
    env = geom.GetEnvelope()
    return int(math.floor(env[2] * 10)), int(math.ceil(env[3] * 10))


def get_times(mets):
    """Get starttimes and endtimes."""

    starttimes = []
    endtimes = []
    for met in mets:
         insort(starttimes, met['starttime'])
         insort(endtimes, met['endtime'])
    return starttimes, endtimes


def create_dataset_json(id, version, location, starttime, endtime, ds_json_file):
    """Create HySDS dataset json file."""

    # build dataset
    ds = {
        'creation_timestamp': "%sZ" % datetime.utcnow().isoformat(),
        'version': version,
        'label': id,
        'location': location,
        'starttime': starttime.isoformat(),
        'endtime': endtime.isoformat(),
    }

    # write out dataset json
    with open(ds_json_file, 'w') as f:
        json.dump(ds, f, indent=2)


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
    }

    # write out dataset json
    with open(met_json_file, 'w') as f:
        json.dump(met, f, indent=2)


def ifg_exists(es_url, es_index, id):
    """Check interferogram exists in GRQ."""

    total, id = check_int(es_url, es_index, id)
    if total > 0: return True
    return False


def call_noerr(cmd):
    """Run command and warn if exit status is not 0."""

    try: check_call(cmd, shell=True)
    except Exception as e:
        logger.warn("Got exception running {}: {}".format(cmd, str(e)))
        logger.warn("Traceback: {}".format(traceback.format_exc()))


class SetEncoder(json.JSONEncoder):
   def default(self, obj):
      if isinstance(obj, set):
         return list(obj)
      return json.JSONEncoder.default(self, obj)


def group_ifgs(hits):
    """Group interferograms by track, direction, swath and platform."""

    # filter on v1.1.2 or later (S1-IFG name scheme change)
    version_re = re.compile(r'v1\.(?:1\.[2-9]|[2-9](?:\.\d+)?)$')

    grouped = {}
    for h in hits:
        #logger.info(json.dumps(h, indent=2))

        id = h['fields']['partial'][0]['id']

        # filter S1-IFGs only
        if h['_type'] != "S1-IFG":
            logger.info("Skipping {}: Invalid type ({}).".format(id, h['_type']))
            continue 

        # filter out old versions
        v = h['fields']['partial'][0]['system_version']
        if version_re.search(v) is None:
            logger.info("Skipping {}: Invalid version ({}).".format(id, v))
            continue 

        # filter out missing track numbers
        track = h['fields']['partial'][0]['metadata']['trackNumber']
        if h['fields']['partial'][0]['metadata']['trackNumber'] is None:
            logger.info("Skipping {}: Invalid trackNumber ({}).".format(id, track))
            continue

        # get metadata
        md = h['fields']['partial'][0]['metadata']

        #id, version, start/end time and url are not part of the metadata, so add it
        md['id'] = id
        md['version'] = v
        md['starttime'] = h['fields']['partial'][0]['starttime']
        md['endtime'] = h['fields']['partial'][0]['endtime']
        md['url'] = h['fields']['partial'][0]['urls'][0]

        # cleanup direction
        if md['direction'] == "asc": direction = "ascending"
        elif md['direction'] == "dsc": direction = "descending"
        else: direction = md['direction']

        gp = grouped.setdefault(track, {}).setdefault(direction, {})
        gp.setdefault('swath', set()).add(md['swath'])
        gp.setdefault('platform', set()).update(md['platform'])
        gp.setdefault('mets', []).append(md)
    grouped = json.loads(json.dumps(grouped, cls=SetEncoder))
    #logger.info(json.dumps(grouped, indent=2))
    return grouped


def query_hits(uu, query):
    """Query hits."""

    # query docs
    logger.info("rest_url: {}".format(uu.rest_url))
    logger.info("dav_url: {}".format(uu.dav_url))
    logger.info("version: {}".format(uu.version))
    logger.info("grq_index_prefix: {}".format(uu.grq_index_prefix))

    # get normalized rest url
    rest_url = uu.rest_url[:-1] if uu.rest_url.endswith('/') else uu.rest_url

    # get index name and url
    url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, uu.grq_index_prefix)
    logger.info("idx: {}".format(uu.grq_index_prefix))
    logger.info("url: {}".format(url))

    # query hits
    query.update({
        "partial_fields" : {
            "partial" : {
                "exclude" : "city"
            }
        }
    })
    #logger.info("query: {}".format(json.dumps(query, indent=2)))
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits


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
    query = ctx['query']
    conf = ctx.get('conf', 'settings.conf')
    sys_ver = ctx.get('sys_ver', "v1*")
    output_file = 'valid_ifg_out.json'
    meta_file = 'valid_meta_out.json'

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

    # create validate_ifg output for each input list
    for inps in inps_list:

        # get times
        starttimes, endtimes = get_times(inps['mets'])
        starttime = datetime.strptime(starttimes[0], "%Y-%m-%dT%H:%M:%S%f")
        endtime = datetime.strptime(endtimes[-1], "%Y-%m-%dT%H:%M:%S%f")

        # md5 hash
        md5 = hashlib.md5(json.dumps(inps, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()

        # get id base
        id_base = "S1-VALIDATED_IFG_STACK-TN{}_{}-{}_{}-{}_{}_s{}-{}".format(inps['track'],
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
        es_index = "{}_{}_s1-validated_ifg_stack".format(uu.grq_index_prefix, version)

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

        # run validate_ifg
        try:
            mets = inps['mets']
            json.dump(mets,open(inps['meta_file'],'w'), indent=2, sort_keys=True)
            json.dump(inps,open('valid_ifg_in.json','w'), indent=2, sort_keys=True)
            main_st(('-a validate_ifg_met -i ' + 'valid_ifg_in.json').split())

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
