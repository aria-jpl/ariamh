#!/usr/bin/env python
"""
Determine all topsApp configurations that can be reprocessed with a specific precise orbit"
"""
from __future__ import division

from builtins import range
from past.utils import old_div
import os, sys, re, requests, json, logging, traceback, argparse
import hashlib
from itertools import product, chain
from datetime import datetime
from requests.packages.urllib3.exceptions import (InsecureRequestWarning,
                                                  InsecurePlatformWarning)

from utils.UrlUtils import UrlUtils as UU
from utils.time_utils import getDatetimeFromString

from enumerate_topsapp_cfgs import (SLC_RE, IFG_ID_TMPL, RSP_ID_TMPL, get_bool_param)


# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('enumerate_ifgs_to_reprocess')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


# regexes
ORBIT_RE = re.compile(r'^(?P<sat>S1.+?)_OPER_AUX_(?P<type>.*?)_OPOD_(?P<cr_yr>\d{4})(?P<cr_mo>\d{2})(?P<cr_dy>\d{2})T(?P<cr_hh>\d{2})(?P<cr_mm>\d{2})(?P<cr_ss>\d{2})_V(?P<vs_yr>\d{4})(?P<vs_mo>\d{2})(?P<vs_dy>\d{2})T(?P<vs_hh>\d{2})(?P<vs_mm>\d{2})(?P<vs_ss>\d{2})_(?P<ve_yr>\d{4})(?P<ve_mo>\d{2})(?P<ve_dy>\d{2})T(?P<ve_hh>\d{2})(?P<ve_mm>\d{2})(?P<ve_ss>\d{2})\.EOF$')
POEORB_RE = re.compile(r'POEORB')


def get_orbit_datetimes(orbit_file):
    """Return validity starttime and endtime for an orbit file."""

    # extract info from orbit filename
    match = ORBIT_RE.search(orbit_file)
    if not match:
        raise RuntimeError("Failed to extract info from orbit filename %s." % id)
    info = match.groupdict()

    # get dates
    create_dt = datetime(*[int(info[i]) for i in ['cr_yr', 'cr_mo', 'cr_dy', 'cr_hh', 'cr_mm', 'cr_ss']])
    valid_start = datetime(*[int(info[i]) for i in ['vs_yr', 'vs_mo', 'vs_dy', 'vs_hh', 'vs_mm', 'vs_ss']])
    valid_end = datetime(*[int(info[i]) for i in ['ve_yr', 've_mo', 've_dy', 've_hh', 've_mm', 've_ss']])
    valid_mid = valid_start + old_div((valid_end - valid_start),2)
    #logger.info("create date:         %s" % create_dt)
    #logger.info("validity start date: %s" % valid_start)
    #logger.info("validity mid date:   %s" % valid_mid)
    #logger.info("validity end date:   %s" % valid_end)
    return create_dt, valid_start, valid_mid, valid_end


def get_orbit_date(orbit_file):
    """Return datetime for orbit's day."""

    cd, vs, vm, ve = get_orbit_datetimes(orbit_file)
    return datetime(vm.year, vm.month, vm.day, 0, 0, 0)


def get_topsapp_cfgs(context_file, id_tmpl=IFG_ID_TMPL):
    """Return all possible topsApp configurations that can be reprocessed with precise orbit."""
    # get context
    with open(context_file) as f:
        context = json.load(f)

    # get dataset type to query
    if id_tmpl == IFG_ID_TMPL: dataset = "S1-IFG"
    elif id_tmpl == RSP_ID_TMPL: dataset = "S1-SLCP"
    else: raise RuntimeError("Failed to recognize dataset from id template: %s" % id_tmpl)

    # get params
    ifg_version = context['ifg_version']
    starttime = context['starttime']
    endtime = context['endtime']
    orb_ds_url = context['url']
    orb_file = context['orbit_file']
    platform = context['platform']

    # get precise orbit date
    orb_dt = get_orbit_date(orb_file)

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

    # build query
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "dataset.raw": dataset
                        }
                    },
                    {
                        "term": {
                            "version.raw": ifg_version
                        }
                    },
                    {
                        "term": {
                            "metadata.orbit_type.raw": "resorb"
                        }
                    },
                    {
                        "term": {
                            "metadata.platform.raw": platform
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {
                                    "range": {
                                        "starttime": {
                                            "from": starttime,
                                            "to": endtime
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "endtime": {
                                            "from": starttime,
                                            "to": endtime
                                        }
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        },
        "partial_fields" : {
            "partial" : {
                "exclude" : "city",
            }
        }
    }
    logger.info("query: {}".format(json.dumps(query, indent=2)))
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
    #logger.info("hits: {}".format(json.dumps(hits, indent=2)))
    logger.info("hits count: {}".format(len(hits)))

    # collect topsapps cfgs
    projects = []
    stitched_args = []
    auto_bboxes = []
    master_zip_urls = []
    slave_zip_urls = []
    swathnums = []
    bboxes = []
    master_orbit_urls = []
    slave_orbit_urls = []
    ifg_ids = []
    for hit in hits:
        # propagate unmodified params
        ifg_ctx = hit['fields']['partial'][0]['metadata']['context']
        sfl_ifg_ctx = ifg_ctx.get('context', {})

        # old id
        ifg_id = ifg_ctx['id']

        # determine orbit to replace
        logger.info("latest precise orbit file date: {}".format(orb_dt.isoformat('T')))
        mo_dt = get_orbit_date(ifg_ctx['master_orbit_file'])
        logger.info("original master orbit file date: {}".format(mo_dt.isoformat('T')))
        so_dt = get_orbit_date(ifg_ctx['slave_orbit_file'])
        logger.info("original slave orbit file date: {}".format(so_dt.isoformat('T')))
        if orb_dt == mo_dt:
            master_orbit_urls.append(os.path.join(orb_ds_url, orb_file))
            slave_orbit_urls.append(ifg_ctx['slave_orbit_url'])
        elif orb_dt == so_dt:
            master_orbit_urls.append(ifg_ctx['master_orbit_url'])
            slave_orbit_urls.append(os.path.join(orb_ds_url, orb_file))
        else:
            logger.info("Precise orbit file {} doesn't align with S1-IFG {}. Skipping.".format(orb_file, ifg_id))
            continue

        logger.info("sfl_ifg_ctx: {}".format(json.dumps(sfl_ifg_ctx, indent=2)))
    
        # carry over the rest of the params
        projects.append(ifg_ctx['project'])
        stitched_args.append(False if len(ifg_ctx['master_zip_url']) == 1 or len(ifg_ctx['slave_zip_url']) == 1 else True)
        auto_bboxes.append(ifg_ctx['auto_bbox'])
        master_zip_urls.append(ifg_ctx['master_zip_url'])
        slave_zip_urls.append(ifg_ctx['slave_zip_url'])
        swathnums.append(ifg_ctx['swathnum'])
        bboxes.append(ifg_ctx['bbox'])

        # determine orbit type of product in case both master and slave orbits were restituted
        if POEORB_RE.search(master_orbit_urls[-1]) and POEORB_RE.search(slave_orbit_urls[-1]):
            ifg_id = ifg_id.replace('resorb', 'poeorb')        

        # calculate hash and new ifg id
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
            ifg_ctx.get('azimuth_looks', sfl_ifg_ctx.get('azimuth_looks', 3)),
            ifg_ctx.get('range_looks', sfl_ifg_ctx.get('range_looks', 7)),
            ifg_ctx.get('filter_strength', sfl_ifg_ctx.get('filter_strength', 0.5)),
            ifg_ctx.get('dem_type', sfl_ifg_ctx.get('dem_type', 'SRTM')),
        ])).hexdigest()
        ifg_id = ifg_id[0:-4] + ifg_hash[0:4]
        ifg_ids.append(ifg_id)

    logger.info("Found {} {} datasets to reprocess.".format(len(ifg_ids), dataset))

    return ( projects, stitched_args, auto_bboxes, ifg_ids, master_zip_urls,
             master_orbit_urls, slave_zip_urls, slave_orbit_urls, swathnums,
             bboxes )


def get_topsapp_cfgs_rsp(context_file):
    """Return all possible topsApp configurations for registered SLC pair products
       that can be reprocessed with precise orbit."""

    return get_topsapp_cfgs(context_file, id_tmpl=RSP_ID_TMPL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("context_file", help="context file")
    args = parser.parse_args()
    ifg_id_dict = {}
    try:
        cfgs = get_topsapp_cfgs(args.context_file)
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
            if cfgs[3][i] in ifg_id_dict: raise RuntimeError("ifg %s already found." % cfgs[3][i])
            ifg_id_dict[cfgs[3][i]] = True
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("{}\n".format(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("{}\n".format(traceback.format_exc()))
        raise
