#!/usr/bin/env python
"""
Determine all combinations of topsApp configurations for most recent pre-event scene"
"""

import os, sys, re, requests, json, logging, traceback, argparse
from copy import deepcopy
from datetime import datetime
from pprint import pformat
from osgeo import ogr, osr

from utils.UrlUtils import UrlUtils as UU

from enumerate_topsapp_cfgs import (SLC_RE, IFG_ID_TMPL, RSP_ID_TMPL,
get_bool_param, get_pair_direction, group_frames_by_track_date, 
dedup_reprocessed_slcs, get_pair_hit_query)
from enumerate_topsapp_cfgs import get_topsapp_cfgs as gtc


# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('get_mrpe_topsapp_cfg')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


def get_dt(dt):
    """Return datetime object from string."""

    return datetime.strptime(dt[:-1] if dt.endswith('Z') else dt, "%Y-%m-%dT%H:%M:%S.%f")


def get_mrpe_hits(rest_url, ref_scene, start_time, event_time):
    """Return most recent pre-event hit."""

    # get datetimes
    start_time = get_dt(start_time)
    event_time = get_dt(event_time)

    # get query url
    url = "{}/grq_*_s1-iw_slc/_search?search_type=scan&scroll=60&size=100".format(rest_url)

    # check SLC id format
    for i in ref_scene['id']:
        match = SLC_RE.search(i)
        if not match:
            raise RuntimeError("Failed to recognize SLC ID %s." % i)

    # get query
    logger.info("=" * 80)
    logger.info("query start/stop dates: {} {}".format(start_time, event_time))
    sort_order = "desc"
    query = get_pair_hit_query(ref_scene['track'], start_time, event_time, 
                               sort_order, ref_scene['location']['coordinates'])

    logger.info("query: {}".format(json.dumps(query, indent=2)))
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    logger.info("total matches: {}".format(scan_result['hits']['total']))
    scroll_id = scan_result['_scroll_id']
    matches = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        matches.extend(res['hits']['hits'])
    sorted_matches = sorted(matches, key=lambda x: x['fields']['partial'][0]['starttime'], reverse=True)
    #logger.info("sorted_matches: {}".format(json.dumps(sorted_matches, indent=2)))
    logger.info("sorted_matches: {}".format([m['_id'] for m in sorted_matches]))
    mrpe_scene_dt = get_dt(sorted_matches[0]['fields']['partial'][0]['starttime'])
    mrpe_dt = datetime(mrpe_scene_dt.year, mrpe_scene_dt.month, mrpe_scene_dt.day, 0, 0, 0)
    mrpe_matches = [ sorted_matches[0] ]
    for match in sorted_matches[1:]:
        match_scene_dt = get_dt(match['fields']['partial'][0]['starttime'])
        match_dt = datetime(match_scene_dt.year, match_scene_dt.month, match_scene_dt.day, 0, 0, 0)
        if match_dt == mrpe_dt:
            mrpe_matches.append(match) 
    #logger.info("mrpe_matches: {}".format(json.dumps(mrpe_matches, indent=2)))
    logger.info("mrpe_matches: {}".format([m['_id'] for m in mrpe_matches]))
    return mrpe_matches


def get_topsapp_cfgs(context_file, temporalBaseline=72, id_tmpl=IFG_ID_TMPL, minMatch=0, covth=.95):
    """Return all possible topsApp configurations."""
    # get context
    with open(context_file) as f:
        context = json.load(f)

    # get args
    event_time = context['event_time']
    start_time = context['start_time']
    end_time = context['end_time']
    project = context['project']
    sso = get_bool_param(context, 'singlesceneOnly')
    auto_bbox = get_bool_param(context, 'auto_bbox')
    precise_orbit_only = get_bool_param(context, 'precise_orbit_only')
    query = context['query']

    # pair direction:
    #   forward => reference scene is slave
    #   backward => reference scene is master
    pre_ref_pd = get_pair_direction(context, 'preReferencePairDirection')
    pre_search = False if pre_ref_pd == 'none' else True
    post_ref_pd = get_pair_direction(context, 'postReferencePairDirection')
    post_search = False if post_ref_pd == 'none' else True

    # overwrite temporal baseline from context
    if 'temporalBaseline' in context:
        temporalBaseline = int(context['temporalBaseline'])

    # overwrite minMatch
    if 'minMatch' in context:
        minMatch = int(context['minMatch'])

    # overwrite covth
    if 'covth' in context:
        covth = float(context['covth'])

    # log enumerator params
    logging.info("event_time: %s" % event_time)
    logging.info("start_time: %s" % start_time)
    logging.info("end_time: %s" % end_time)
    logging.info("project: %s" % project)
    logging.info("singleceneOnly: %s" % sso)
    logging.info("auto_bbox: %s" % auto_bbox)
    logging.info("preReferencePairDirection: %s" % pre_ref_pd)
    logging.info("postReferencePairDirection: %s" % post_ref_pd)
    logging.info("temporalBaseline: %s" % temporalBaseline)
    logging.info("minMatch: %s" % minMatch)
    logging.info("covth: %s" % covth)

    # get bbox from query
    coords = None
    bbox = [-90., 90., -180., 180.]
    if 'and' in query.get('query', {}).get('filtered', {}).get('filter', {}):
        filts = query['query']['filtered']['filter']['and']
    elif 'geo_shape' in query.get('query', {}).get('filtered', {}).get('filter', {}):
        filts = [ { "geo_shape": query['query']['filtered']['filter']['geo_shape'] } ]
    else: filts = []
    for filt in filts:
        if 'geo_shape' in filt:
            coords = filt['geo_shape']['location']['shape']['coordinates']
            roi = {
                'type': 'Polygon',
                'coordinates': coords,
            }
            logger.info("query filter ROI: %s" % json.dumps(roi))
            roi_geom = ogr.CreateGeometryFromJson(json.dumps(roi))
            roi_x_min, roi_x_max, roi_y_min, roi_y_max = roi_geom.GetEnvelope()
            bbox = [ roi_y_min, roi_y_max, roi_x_min, roi_x_max ]
            logger.info("query filter bbox: %s" % bbox)
            break

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

    # query hits
    query.update({
        "partial_fields" : {
            "partial" : {
                "exclude" : "city",
            }
        }
    })
    #logger.info("query: {}".format(json.dumps(query, indent=2)))
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    ref_hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        ref_hits.extend(res['hits']['hits'])

    # extract reference ids
    ref_ids = { h['_id']: True for h in ref_hits }
    logger.info("ref_ids: {}".format(json.dumps(ref_ids, indent=2)))
    logger.info("ref_hits count: {}".format(len(ref_hits)))

    # group ref hits by track and date
    grouped_refs = group_frames_by_track_date(ref_hits)

    # dedup any reprocessed reference SLCs
    dedup_reprocessed_slcs(grouped_refs['grouped'], grouped_refs['metadata'])

    #logger.info("ref hits: {}".format(json.dumps(grouped_refs['hits'], indent=2)))
    #logger.info("ref sorted_hits: {}".format(pformat(grouped_refs['grouped'])))
    #logger.info("ref slc_dates: {}".format(pformat(grouped_refs['dates'])))
    #logger.info("ref slc_footprints: {}".format(json.dumps(grouped_refs['footprints'], indent=2)))

    # build list reference scenes
    ref_scenes = []
    for track in grouped_refs['grouped']:
        logger.info("track: %s" % track)
        for ref_dt in grouped_refs['grouped'][track]:
            logger.info("reference date: %s" % ref_dt.isoformat())
            if sso:
                for ref_id in grouped_refs['grouped'][track][ref_dt]:
                    ref_scenes.append({ 'id': [ ref_id ],
                                        'track': track,
                                        'date': ref_dt,
                                        'location': grouped_refs['footprints'][ref_id],
                                        'pre_matches': None,
                                        'post_matches': None })
            else:
                union_poly = get_union_geometry(grouped_refs['grouped'][track][ref_dt],
                                                grouped_refs['footprints'])
                if len(union_poly['coordinates']) > 1:
                    logger.warn("Stitching %s will result in a disjoint geometry." % grouped_refs['grouped'][track][ref_dt])
                    logger.warn("Skipping.")
                else:
                    ref_scenes.append({ 'id': grouped_refs['grouped'][track][ref_dt],
                                        'track': track,
                                        'date': ref_dt,
                                        'location': union_poly,
                                        'pre_matches': None,
                                        'post_matches': None })

    # find reference scene matches
    projects = []
    stitched_args = []
    auto_bboxes = []
    ifg_ids = []
    master_zip_urls = []
    master_orbit_urls = []
    slave_zip_urls = []
    slave_orbit_urls = []
    swathnums = []
    bboxes = []
    mrpe_dict = {}
    for ref_scene in ref_scenes:
        for ref_id in ref_scene['id']:
            logger.info("#" * 80)
            logger.info("ref id: %s" % ref_id)
            logger.info("ref date: %s" % ref_scene['date'])
            logger.info("ref scene: %s" % pformat(ref_scene))
            mrpe_hits = get_mrpe_hits(rest_url, ref_scene, start_time, event_time)
            for mrpe_hit in mrpe_hits:
                if mrpe_hit['_id'] in mrpe_dict: continue
                mrpe_dict[mrpe_hit['_id']] = True
                logger.info("mrpe_hit: %s" % pformat(mrpe_hit))
                new_query = {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "_id": mrpe_hit['_id'],
                                    }
                                },
                                {
                                    "term": {
                                      "system_version.raw": mrpe_hit['fields']['partial'][0]['system_version'],
                                    }
                                }
                            ]
                        }
                    }
                }
                new_context = deepcopy(context)
                new_context['query'] = new_query
                tmp_ctx_file = "%s.context.json" % ref_id
                with open('%s.context.json' % ref_id, 'w') as f:
                    json.dump(new_context, f, indent=2)
                (tmp_projects, tmp_stitched_args, tmp_auto_bboxes, tmp_ifg_ids, tmp_master_zip_urls,
                tmp_master_orbit_urls, tmp_slave_zip_urls, tmp_slave_orbit_urls, tmp_swathnums,
                tmp_bboxes) = gtc(tmp_ctx_file, temporalBaseline=temporalBaseline, id_tmpl=id_tmpl,
                                  minMatch=minMatch, covth=covth)
                projects.extend(tmp_projects)
                stitched_args.extend(tmp_stitched_args)
                auto_bboxes.extend(tmp_auto_bboxes)
                ifg_ids.extend(tmp_ifg_ids)
                master_zip_urls.extend(tmp_master_zip_urls)
                master_orbit_urls.extend(tmp_master_orbit_urls)
                slave_zip_urls.extend(tmp_slave_zip_urls)
                slave_orbit_urls.extend(tmp_slave_orbit_urls)
                swathnums.extend(tmp_swathnums)
                bboxes.extend(tmp_bboxes)

    return ( projects, stitched_args, auto_bboxes, ifg_ids, master_zip_urls,
             master_orbit_urls, slave_zip_urls, slave_orbit_urls, swathnums,
             bboxes )


def get_topsapp_cfgs_rsp(context_file, temporalBaseline=72):
    """Return all possible topsApp configurations for registered SLC pair products."""

    return get_topsapp_cfgs(context_file, temporalBaseline, id_tmpl=RSP_ID_TMPL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("context_file", help="context file")
    parser.add_argument("-t", "--temporalBaseline", dest="temporalBaseline",
                        type=int, default=72, help="temporal baseline")
    args = parser.parse_args()
    ifg_id_dict = {}
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
            if cfgs[3][i] in ifg_id_dict: raise RuntimeError("ifg %s already found." % cfgs[3][i])
            ifg_id_dict[cfgs[3][i]] = True
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("{}\n".format(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("{}\n".format(traceback.format_exc()))
        raise
