#!/usr/bin/env python
"""
Generate configuration for a topsApp.py run.
"""

from builtins import range
import os, sys, re, requests, json, logging, traceback, argparse
import hashlib
from itertools import product, chain
from datetime import datetime, timedelta
import numpy as np
from osgeo import ogr, osr
from requests.packages.urllib3.exceptions import (InsecureRequestWarning,
                                                  InsecurePlatformWarning)

from utils.UrlUtils import UrlUtils as UU
from enumerate_topsapp_cfgs import (RESORB_RE, SLC_RE, IFG_ID_TMPL, RSP_ID_TMPL, get_bool_param)
from fetchOrbitES import fetch


# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('get_topsapp_cfg')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


def get_metadata(id, rest_url, url):
    """Get SLC metadata."""

    # query hits
    query = {
        "query": {
            "term": {
                "_id": id,
            },
        },
        "partial_fields" : {
            "partial" : {
                "exclude" : "city",
            }
        }
    }
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
    if len(hits) == 0:
        raise RuntimeError("Failed to find {}.".format(id))
    return hits[0]


def get_dates_mission(id):
    """Return day date, slc start date and slc end date."""

    match = SLC_RE.search(id)
    if not match:
        raise RuntimeError("Failed to recognize SLC ID %s." % id)
    day_dt = datetime(int(match.group('start_year')),
                      int(match.group('start_month')),
                      int(match.group('start_day')),
                      0, 0, 0)
    slc_start_dt = datetime(int(match.group('start_year')),
                            int(match.group('start_month')),
                            int(match.group('start_day')),
                            int(match.group('start_hour')),
                            int(match.group('start_min')),
                            int(match.group('start_sec')))
    slc_end_dt = datetime(int(match.group('end_year')),
                          int(match.group('end_month')),
                          int(match.group('end_day')),
                          int(match.group('end_hour')),
                          int(match.group('end_min')),
                          int(match.group('end_sec')))
    mission = match.group('mission')
    return day_dt, slc_start_dt, slc_end_dt, mission


def get_ifg_dates(master_ids, slave_ids):
    """Return ifg start and end dates."""

    master_day_dts = {}
    for id in master_ids:
        day_dt, slc_start_dt, slc_end_dt, mission = get_dates_mission(id)
        master_day_dts.setdefault(day_dt, []).extend([slc_start_dt, slc_end_dt])
    if len(master_day_dts) > 1:
        raise RuntimeError("Found master SLCs for more than 1 day.")
    master_day_dt = day_dt
    master_all_dts = master_day_dts[day_dt]
    master_all_dts.sort()

    slave_day_dts = {}
    for id in slave_ids:
        day_dt, slc_start_dt, slc_end_dt, mission = get_dates_mission(id)
        slave_day_dts.setdefault(day_dt, []).extend([slc_start_dt, slc_end_dt])
    if len(slave_day_dts) > 1:
        raise RuntimeError("Found slave SLCs for more than 1 day.")
    slave_day_dt = day_dt
    slave_all_dts = slave_day_dts[day_dt]
    slave_all_dts.sort()

    if master_day_dt < slave_day_dt: return master_all_dts[0], slave_all_dts[-1]
    else: return master_all_dts[-1], slave_all_dts[0]


def get_orbit(ids):
    """Get orbit for a set of SLC ids. They need to belong to the same day."""

    day_dts = {}
    if len(ids) == 0: raise RuntimeError("No SLC ids passed.")
    for id in ids:
        day_dt, slc_start_dt, slc_end_dt, mission = get_dates_mission(id)
        day_dts.setdefault(day_dt, []).extend([slc_start_dt, slc_end_dt])
    if len(day_dts) > 1:
        raise RuntimeError("Found SLCs for more than 1 day.")
    all_dts = day_dts[day_dt]
    all_dts.sort()
    return fetch("%s.0" % all_dts[0].isoformat(), "%s.0" % all_dts[-1].isoformat(),
                 mission=mission, dry_run=True)


def get_urls(info):
    """Return list of SLC URLs with preference for S3 URLs."""

    urls = []
    for id in info:
        h = info[id]
        fields = h['fields']['partial'][0]
        prod_url = fields['urls'][0]
        if len(fields['urls']) > 1:
            for u in fields['urls']:
                if u.startswith('s3://'):
                    prod_url = u
                    break
        urls.append("%s/%s" % (prod_url, fields['metadata']['archive_filename']))
    return urls


def get_track(info):
    """Get track number."""

    tracks = {}
    for id in info:
        h = info[id]
        fields = h['fields']['partial'][0]   
        track = fields['metadata']['trackNumber']
        tracks.setdefault(track, []).append(id)
    if len(tracks) != 1:
        raise RuntimeError("Failed to find SLCs for only 1 track.")
    return track
        

def get_topsapp_cfg(context_file, id_tmpl=IFG_ID_TMPL):
    """Return all possible topsApp configurations."""
    # get context
    with open(context_file) as f:
        context = json.load(f)

    # get args
    project = context['project']
    master_ids = [i.strip() for i in context['master_ids'].split()]
    slave_ids = [i.strip() for i in context['slave_ids'].split()]
    subswaths = [int(i.strip()) for i in context['subswaths'].split()]
    azimuth_looks = int(context['azimuth_looks'])
    range_looks = int(context['range_looks'])
    filter_strength = float(context['filter_strength'])
    precise_orbit_only = get_bool_param(context, 'precise_orbit_only')

    # log inputs
    logger.info("project: {}".format(project))
    logger.info("master_ids: {}".format(master_ids))
    logger.info("slave_ids: {}".format(slave_ids))
    logger.info("subswaths: {}".format(subswaths))
    logger.info("azimuth_looks: {}".format(azimuth_looks))
    logger.info("range_looks: {}".format(range_looks))
    logger.info("filter_strength: {}".format(filter_strength))
    logger.info("precise_orbit_only: {}".format(precise_orbit_only))

    # query docs
    uu = UU()
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

    # get metadata
    master_md = { i:get_metadata(i, rest_url, url) for i in master_ids }
    #logger.info("master_md: {}".format(json.dumps(master_md, indent=2)))
    slave_md = { i:get_metadata(i, rest_url, url) for i in slave_ids }
    #logger.info("slave_md: {}".format(json.dumps(slave_md, indent=2)))

    # get tracks
    track = get_track(master_md)
    logger.info("master_track: {}".format(track))
    slave_track = get_track(slave_md)
    logger.info("slave_track: {}".format(slave_track))
    if track != slave_track:
        raise RuntimeError("Slave track {} doesn't match master track {}.".format(slave_track, track))

    # get urls (prefer s3)
    master_urls = get_urls(master_md) 
    logger.info("master_urls: {}".format(master_urls))
    slave_urls = get_urls(slave_md) 
    logger.info("slave_ids: {}".format(slave_urls))

    # get orbits
    master_orbit_url = get_orbit(master_ids)
    logger.info("master_orbit_url: {}".format(master_orbit_url))
    slave_orbit_url = get_orbit(slave_ids)
    logger.info("slave_orbit_url: {}".format(slave_orbit_url))

    # get orbit type
    orbit_type = 'poeorb'
    for o in (master_orbit_url, slave_orbit_url):
        if RESORB_RE.search(o):
            orbit_type = 'resorb'
            break

    # fail if we expect only precise orbits
    if precise_orbit_only and orbit_type == 'resorb':
        raise RuntimeError("Precise orbit required.")

    # get ifg start and end dates
    ifg_master_dt, ifg_slave_dt = get_ifg_dates(master_ids, slave_ids)

    #submit jobs
    projects = []
    stitched_args = []
    ifg_ids = []
    master_zip_urls = []
    master_orbit_urls = []
    slave_zip_urls = []
    slave_orbit_urls = []
    swathnums = []
    bboxes = []
    auto_bboxes = []
    orbit_dict = {}

    # generate job configs
    bbox = [-90., 90., -180., 180.]
    auto_bbox = True
    for subswath in subswaths:
        stitched_args.append(False if len(master_ids) == 1 or len(slave_ids) == 1 else True)
        master_zip_urls.append(master_urls)
        master_orbit_urls.append(master_orbit_url)
        slave_zip_urls.append(slave_urls)
        slave_orbit_urls.append(slave_orbit_url)
        swathnums.append(subswath)
        bboxes.append(bbox)
        auto_bboxes.append(auto_bbox)
        projects.append(project)
        ifg_hash = hashlib.md5(json.dumps([
            id_tmpl,
            stitched_args[-1],
            master_zip_urls[-1],
            master_orbit_urls[-1],
            slave_zip_urls[-1],
            slave_orbit_urls[-1],
            swathnums[-1],
            #bboxes[-1],
            #auto_bboxes[-1],
            projects[-1],
            azimuth_looks,
            range_looks,
            filter_strength,
        ])).hexdigest()
        ifg_ids.append(id_tmpl.format('M', len(master_ids), len(slave_ids),
                                      track, ifg_master_dt,
                                      ifg_slave_dt, subswath,
                                      orbit_type, ifg_hash[0:4]))
                            

    return ( projects, stitched_args, auto_bboxes, ifg_ids, master_zip_urls,
             master_orbit_urls, slave_zip_urls, slave_orbit_urls, swathnums,
             bboxes )


def get_topsapp_cfg_rsp(context_file):
    """Return all possible topsApp configurations for registered SLC pair products."""

    return get_topsapp_cfg(context_file, id_tmpl=RSP_ID_TMPL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("context_file", help="context file")
    args = parser.parse_args()
    ifg_id_dict = {}
    try:
        cfgs = get_topsapp_cfg(args.context_file)
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
            if cfgs[3][i] in ifg_id_dict: raise RuntimeError("ifg %s already found." % cfgs[3][i])
            ifg_id_dict[cfgs[3][i]] = True
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("{}\n".format(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("{}\n".format(traceback.format_exc()))
        raise
