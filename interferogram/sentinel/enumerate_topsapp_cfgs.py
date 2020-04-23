#!/usr/bin/env python
"""
Determine all combinations of topsApp configurations"
"""
from __future__ import division

from builtins import str
from builtins import range
from past.utils import old_div
import os, sys, re, requests, json, logging, traceback, argparse, copy, bisect
import hashlib
from itertools import product, chain
from datetime import datetime, timedelta
import numpy as np
from osgeo import ogr, osr
from requests.packages.urllib3.exceptions import (InsecureRequestWarning,
                                                  InsecurePlatformWarning)
from pprint import pformat

import isce
from utils.UrlUtils import UrlUtils as UU

#from fetchOrbit import fetch
from fetchOrbitES import fetch


# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('enumerate_topsapp_cfgs')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


RESORB_RE = re.compile(r'_RESORB_')

SLC_RE = re.compile(r'(?P<mission>S1\w)_IW_SLC__.*?' +
                    r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'_(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_.*$')

IFG_ID_TMPL = "S1-IFG_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s{}-{}-{}"
RSP_ID_TMPL = "S1-SLCP_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s{}-{}-{}"


def get_overlap(loc1, loc2):
    """Return percent overlap of two GeoJSON geometries."""

    # geometries are in lat/lon projection
    src_srs = osr.SpatialReference()
    src_srs.SetWellKnownGeogCS("WGS84")
    #src_srs.ImportFromEPSG(4326)

    # use projection with unit as meters
    tgt_srs = osr.SpatialReference()
    tgt_srs.ImportFromEPSG(3857)

    # create transformer
    transform = osr.CoordinateTransformation(src_srs, tgt_srs)
    
    # get area of first geometry
    geom1 = ogr.CreateGeometryFromJson(json.dumps(loc1))
    geom1.Transform(transform)
    logger.info("geom1: %s" % geom1)
    area1 = geom1.GetArea() # in square meters
    logger.info("area (m^2) for geom1: %s" % area1)
    
    # get area of second geometry
    geom2 = ogr.CreateGeometryFromJson(json.dumps(loc2))
    geom2.Transform(transform)
    logger.info("geom2: %s" % geom2)
    area2 = geom2.GetArea() # in square meters
    logger.info("area (m^2) for geom2: %s" % area2)
    
    # get area of intersection
    intersection = geom1.Intersection(geom2)
    intersection.Transform(transform)
    logger.info("intersection: %s" % intersection)
    intersection_area = intersection.GetArea() # in square meters
    logger.info("area (m^2) for intersection: %s" % intersection_area)
    if area1 > area2:
        return old_div(intersection_area,area1)
    else:
        return old_div(intersection_area,area2)
    

def get_union_geometry(ids, footprints):
    """Return polygon of union of SLC footprints."""

    # geometries are in lat/lon projection
    src_srs = osr.SpatialReference()
    src_srs.SetWellKnownGeogCS("WGS84")
    #src_srs.ImportFromEPSG(4326)

    # get union geometry of all scenes
    geoms = []
    union = None
    ids.sort()
    for id in ids:
        geom = ogr.CreateGeometryFromJson(json.dumps(footprints[id]))
        geoms.append(geom)
        union = geom if union is None else union.Union(geom)
    union_geojson =  json.loads(union.ExportToJson())
    return union_geojson
            

#def truncated_stitch(m_ids, s_ids, slc_footprints, coords=None, covth=.95):
def ref_truncated(ref_scene, ids, footprints, covth=.95):
    """Return True if reference scene will be truncated."""

    # geometries are in lat/lon projection
    src_srs = osr.SpatialReference()
    src_srs.SetWellKnownGeogCS("WGS84")
    #src_srs.ImportFromEPSG(4326)

    # use projection with unit as meters
    tgt_srs = osr.SpatialReference()
    tgt_srs.ImportFromEPSG(3857)

    # create transformer
    transform = osr.CoordinateTransformation(src_srs, tgt_srs)
    
    # get polygon to fill if specified
    ref_geom = ogr.CreateGeometryFromJson(json.dumps(ref_scene['location']))
    ref_geom_tr = ogr.CreateGeometryFromJson(json.dumps(ref_scene['location']))
    ref_geom_tr.Transform(transform)
    ref_geom_tr_area = ref_geom_tr.GetArea() # in square meters
    logger.info("Reference GeoJSON: %s" % ref_geom.ExportToJson())

    # get union geometry of all matched scenes
    matched_geoms = []
    matched_union = None
    matched_geoms_tr = []
    matched_union_tr = None
    ids.sort()
    logger.info("ids: %s" % len(ids))
    for id in ids:
        geom = ogr.CreateGeometryFromJson(json.dumps(footprints[id]))
        geom_tr = ogr.CreateGeometryFromJson(json.dumps(footprints[id]))
        geom_tr.Transform(transform)
        matched_geoms.append(geom)
        matched_geoms_tr.append(geom_tr)
        if matched_union is None:
            matched_union = geom
            matched_union_tr = geom_tr
        else:
            matched_union = matched_union.Union(geom)
            matched_union_tr = matched_union_tr.Union(geom_tr)
    matched_union_geojson =  json.loads(matched_union.ExportToJson())
    logger.info("Matched union GeoJSON: %s" % json.dumps(matched_union_geojson))
    
    # check matched_union disjointness
    if len(matched_union_geojson['coordinates']) > 1:
        logger.info("Matched union is a disjoint geometry.")
        return True
            
    # check that intersection of reference and stitched scenes passes coverage threshold
    ref_int = ref_geom.Intersection(matched_union)
    ref_int_tr = ref_geom_tr.Intersection(matched_union_tr)
    ref_int_tr_area = ref_int_tr.GetArea() # in square meters
    logger.info("Reference intersection GeoJSON: %s" % ref_int.ExportToJson())
    logger.info("area (m^2) for intersection: %s" % ref_int_tr_area)
    cov = old_div(ref_int_tr_area,ref_geom_tr_area)
    logger.info("coverage: %s" % cov)
    if cov < covth:
        logger.info("Matched union doesn't cover at least %s%% of the reference footprint." % (covth*100.))
        return True
   
    return False


def get_pair_hit_query(track, query_start, query_stop, sort_order, coords):
    """Return pair hit query."""

    query = {
        "query": {
            "bool": {
                "must": [
                    # disable match on version to allow matchups across S1-SLC versions
                    #{
                    #    "term": {
                    #        "system_version": system_version
                    #    }
                    #}, 
                    {
                        "term": {
                            "metadata.trackNumber": track
                        }
                    }, 
                    {
                        "bool": {
                            "should": [
                                {
                                    "range": {
                                        "metadata.sensingStart": {
                                            "from": query_start.isoformat(),
                                            "to": query_stop.isoformat()
                                        }
                                    }
                                }, 
                                {
                                    "range": {
                                        "metadata.sensingStop": {
                                            "from": query_start.isoformat(),
                                            "to": query_stop.isoformat()
                                        }
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "starttime": {
                    "order": sort_order
                }
            }
        ], 
        "partial_fields" : {
            "partial" : {
                "exclude" : ["city", "context"],
            }
        }
    }

    # restrict to polygon
    query['query'] = {
        "filtered": {
            "filter": {
                "geo_shape": {
                    "location": {
                        "shape": {
                            "type": "Polygon",
                            "coordinates": coords
                        }
                    }
                }
            },
            "query": query['query'],
        }
    }

    return query


def get_pair_hits(rest_url, ref_scene, direction, temporal_baseline=72, min_match=2, 
                  temporal_baseline_slider=6, temporal_baseline_max=365, covth=0.95):
    """Return hits that will result in single-scene pairs."""

    # check direction
    if direction not in ('pre', 'post'):
        raise RuntimeError("Unknown direction to search: %s" % direction)

    # get query url
    url = "{}/grq_*_s1-iw_slc/_search?search_type=scan&scroll=60&size=100".format(rest_url)

    # check SLC id format
    for i in ref_scene['id']:
        match = SLC_RE.search(i)
        if not match:
            raise RuntimeError("Failed to recognize SLC ID %s." % i)

    # get start/stop dates
    if direction == 'pre':
        query_start = ref_scene['date'] - timedelta(days=temporal_baseline)
        query_stop = ref_scene['date']
        sort_order = 'desc'
        baseline_diff = (ref_scene['date'] - query_start).days
    else:
        query_start = ref_scene['date']
        query_stop = ref_scene['date'] + timedelta(days=temporal_baseline)
        sort_order = 'asc'
        baseline_diff = (query_stop - ref_scene['date']).days
        
    # accumulate matches
    filtered_matches = []
    filtered_dates = {}

    # acquire minimum match
    while True:

        # get query
        logger.info("=" * 80)
        logger.info("query start/stop dates: {} {}".format(query_start, query_stop))
        query = get_pair_hit_query(ref_scene['track'], query_start, query_stop, 
                                   sort_order, ref_scene['location']['coordinates'])

        #logger.info(json.dumps(query, indent=2))
        r = requests.post(url, data=json.dumps(query))
        r.raise_for_status()
        scan_result = r.json()
        logger.info("total matches for {} direction: {}".format(direction, scan_result['hits']['total']))
        scroll_id = scan_result['_scroll_id']
        matches = []
        while True:
            r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
            res = r.json()
            scroll_id = res['_scroll_id']
            if len(res['hits']['hits']) == 0: break
            matches.extend(res['hits']['hits'])
        logger.info("matches: {}".format([m['_id'] for m in matches]))

        # filter matches
        hit_info = {}
        hit_dates = {}
        hit_footprints = {}
        for m in matches:
            h = m['fields']['partial'][0]
            #logger.info("h: {}".format(json.dumps(h, indent=2)))
            if h['id'] in ref_scene['id']:
                logger.info("Filtering self: %s" % h['id'])
                continue
            match = SLC_RE.search(h['id'])
            if not match:
                raise RuntimeError("Failed to recognize SLC ID %s." % h['id'])
            hit_dt = datetime(int(match.group('start_year')),
                              int(match.group('start_month')),
                              int(match.group('start_day')),
                              0, 0, 0)
            if ref_scene['date'] == hit_dt: 
                logger.info("Filtering scenes from reference day: %s" % h['id'])
                continue
            hit_info[h['id']] = m
            hit_footprints[h['id']] = h['location']
            hit_dates.setdefault(hit_dt, []).append(h['id'])

        # filter dates that truncate reference scene
        for hit_date in hit_dates:
            logger.info("-" * 80)
            logger.info("hit_date: %s" % hit_date)
            if not ref_truncated(ref_scene, hit_dates[hit_date], hit_footprints, covth=covth):
                filtered_matches.extend([hit_info[i] for i in hit_dates[hit_date]])
                filtered_dates[hit_date] = hit_dates[hit_date]
                logger.info("Added hit_date %s." % hit_date)
            else:
                logger.info("Not adding hit_date %s." % hit_date)

        # break if min match acquired
        if len(filtered_dates) >= min_match: break

        # raise if searching past temporal baseline max
        logger.info("temporal baseline diff: {}".format(baseline_diff))
        if baseline_diff > temporal_baseline_max:
            #raise RuntimeError("Failed to fulfill minMatch %s." % min_match)
            logger.warn("Reached max temporal baseline while trying to filfill minMatch %s." % min_match)
            logger.warn("Returning what we have.")
            return filtered_matches

        # slide query search time
        logger.info("Minimum match (%s) not yet met (%s). Sliding query times." % (min_match, len(filtered_dates)))
        if direction == 'pre':
            query_stop = query_start
            query_start = query_stop - timedelta(days=temporal_baseline_slider)
            baseline_diff = (ref_scene['date'] - query_start).days
        else:
            query_start = query_stop
            query_stop = query_start + timedelta(days=temporal_baseline_slider)
            baseline_diff = (query_stop - ref_scene['date']).days

    logger.info("total filtered_matches: {}".format(len(filtered_matches)))
    logger.info("filtered_matches: {}".format([fm['_id'] for fm in filtered_matches]))
    logger.info("ref date: {}".format(ref_scene['date']))
    logger.info("filtered dates: {}".format(" ".join([str(i) for i in filtered_dates])))
    logger.info("total filtered dates: {}".format(len(filtered_dates)))

    return filtered_matches


def dedup_reprocessed_slcs(sorted_hits, slc_metadata, ssth=3.):
    """Filter out duplicate SLC scenes. Use the one with the latest IPF version and latest processing time."""

    logger.info("#" * 80)
    logger.info("Running dedup of duplicate/reprocessed SLCs.")
    for track in sorted_hits:
        logger.info("track: %s" % track)
        for day_dt in sorted(sorted_hits[track]):
            logger.info("-" * 80)
            logger.info("day_dt: %s" % day_dt.isoformat())
            dedup_slcs = dict()
            last_id = None
            last_sensing_start = None
            for id in sorted(sorted_hits[track][day_dt]):
                md = slc_metadata[id]
                sensing_start = datetime.strptime(
                    md['sensingStart'][:-1] if md['sensingStart'].endswith('Z') else md['sensingStart'], 
                    "%Y-%m-%dT%H:%M:%S.%f"
                )
                if 'postProcessingStop' not in md: continue
                post_processing_stop = datetime.strptime(
                    md['postProcessingStop'][:-1] if md['postProcessingStop'].endswith('Z') else md['postProcessingStop'],
                    "%Y-%m-%dT%H:%M:%S.%f"
                )
                version = md['version']
                logger.info("+" * 80)
                logger.info("id: %s" % id)
                logger.info("sensing_start: %s" % sensing_start.isoformat())
                logger.info("post_processing_stop: %s" % post_processing_stop.isoformat())
                logger.info("version: %s" % version)
                if last_id is not None:
                    sec_diff = abs((sensing_start-last_sensing_start).total_seconds())
                    logger.info("sec_diff: %s" % sec_diff)
                    if sec_diff < ssth:
                        if post_processing_stop < dedup_slcs[last_id]['postProcessingStop']:
                            logger.info("Filtering older reprocessed SLC: %s" % id)
                            continue
                        elif post_processing_stop > dedup_slcs[last_id]['postProcessingStop']:
                            logger.info("Filtering older reprocessed SLC: %s" % last_id)
                            del dedup_slcs[last_id]
                        elif post_processing_stop == dedup_slcs[last_id]['postProcessingStop']:
                            if version < dedup_slcs[last_id]['version']:
                                logger.info("Filtering older version of reprocessed SLC: %s" % id)
                                continue
                            else:
                                logger.info("Filtering older version reprocessed SLC: %s" % last_id)
                                del dedup_slcs[last_id]
                last_id = id
                last_sensing_start = sensing_start
                dedup_slcs[id] = {
                    'version': version,
                    'postProcessingStop': post_processing_stop,
                }
            sorted_hits[track][day_dt] = sorted(dedup_slcs.keys())


def group_frames_by_track_date(frames):
    """Classify frames by track and date."""

    hits = {}
    grouped = {}
    dates = {}
    footprints = {}
    metadata = {}
    for h in frames: 
        if h['_id'] in hits: continue
        fields = h['fields']['partial'][0]

        # get product url; prefer S3
        prod_url = fields['urls'][0]
        if len(fields['urls']) > 1:
            for u in fields['urls']:
                if u.startswith('s3://'):
                    prod_url = u
                    break

        hits[h['_id']] = "%s/%s" % (prod_url, fields['metadata']['archive_filename'])
        match = SLC_RE.search(h['_id'])
        if not match:
            raise RuntimeError("Failed to recognize SLC ID %s." % h['_id'])
        day_dt = datetime(int(match.group('start_year')),
                          int(match.group('start_month')),
                          int(match.group('start_day')),
                          0, 0, 0)
        bisect.insort(grouped.setdefault(fields['metadata']['trackNumber'], {}) \
                             .setdefault(day_dt, []), h['_id'])
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
        dates[h['_id']] = [ slc_start_dt, slc_end_dt ]
        footprints[h['_id']] = fields['location']
        metadata[h['_id']] = fields['metadata']
    return {
        "hits": hits,
        "grouped": grouped,
        "dates": dates,
        "footprints": footprints,
        "metadata": metadata,
    }


def get_bool_param(ctx, param):
    """Return bool param from context."""

    if param in ctx and isinstance(ctx[param], bool): return ctx[param]
    return True if ctx.get(param, 'true').strip().lower() == 'true' else False


def get_pair_direction(ctx, param):
    """Validate and return pair_direction param from context."""

    if param not in ctx: raise RuntimeError("Failed to find %s in context." % param)
    pd = ctx[param]
    if pd in ('backward', 'forward', 'both', 'none'): return pd
    else: raise RuntimeError("Invalid pair direction %s." % pd)


def get_topsapp_cfgs(context_file, temporalBaseline=72, id_tmpl=IFG_ID_TMPL, minMatch=0, covth=.95):
    """Return all possible topsApp configurations."""
    # get context
    with open(context_file) as f:
        context = json.load(f)

    # get args
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
    for ref_scene in ref_scenes:
        logger.info("#" * 80)
        logger.info("ref id: %s" % ref_scene['id'])
        logger.info("ref date: %s" % ref_scene['date'])
        if pre_search:
            logger.info("*" * 80)
            pre_matches = group_frames_by_track_date(
                              get_pair_hits(rest_url, ref_scene, 'pre',
                                            temporal_baseline=temporalBaseline,
                                            min_match=minMatch, covth=covth)
                          )
            dedup_reprocessed_slcs(pre_matches['grouped'], pre_matches['metadata'])
            ref_scene['pre_matches'] = pre_matches
        if post_search:
            logger.info("*" * 80)
            post_matches = group_frames_by_track_date(
                               get_pair_hits(rest_url, ref_scene, 'post',
                                             temporal_baseline=temporalBaseline,
                                             min_match=minMatch, covth=covth)
                           )
            dedup_reprocessed_slcs(post_matches['grouped'], post_matches['metadata'])
            ref_scene['post_matches'] = post_matches

    #logger.info("ref_scenes: {}".format(pformat(ref_scenes)))
    #logger.info("ref_scenes count: {}".format(len(ref_scenes)))

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
    for ref_scene in ref_scenes:
        ref_ids = ref_scene['id']
        track = ref_scene['track']
        ref_dts = []
        for i in ref_ids: ref_dts.extend(grouped_refs['dates'][i])
        #logger.info("ref_ids: %s" % ref_ids)
        #logger.info("ref_dts: %s" % ref_dts)
            
        # set orbit urls and cache for reference dates
        ref_dt_orb = "%s_%s" % (ref_dts[0].isoformat(), ref_dts[-1].isoformat())
        if ref_dt_orb not in orbit_dict:
            match = SLC_RE.search(ref_ids[0])
            if not match:
                raise RuntimeError("Failed to recognize SLC ID %s." % ref_ids[0])
            mission = match.group('mission')
            orbit_dict[ref_dt_orb] = fetch("%s.0" % ref_dts[0].isoformat(),
                                           "%s.0" % ref_dts[-1].isoformat(),
                                           mission=mission, dry_run=True)
            if orbit_dict[ref_dt_orb] is None:
                raise RuntimeError("Failed to query for an orbit URL for track {} {} {}.".format(track,
                                   ref_dts[0], ref_dts[-1]))

        # generate jobs for pre-reference pairs
        if ref_scene['pre_matches'] is not None:
            if track in ref_scene['pre_matches']['grouped']:
                matched_days = ref_scene['pre_matches']['grouped'][track]
                for matched_day, matched_ids in matched_days.items():
                    matched_dts = []
                    for i in matched_ids: matched_dts.extend(ref_scene['pre_matches']['dates'][i])
                    #logger.info("pre_matches matched_ids: %s" % matched_ids)
                    #logger.info("pre_matches matched_dts: %s" % matched_dts)
                    all_dts = list(chain(ref_dts, matched_dts))
                    all_dts.sort()

                    # set orbit urls and cache for matched dates
                    matched_dt_orb = "%s_%s" % (matched_dts[0].isoformat(), matched_dts[-1].isoformat())
                    if matched_dt_orb not in orbit_dict:
                        match = SLC_RE.search(matched_ids[0])
                        if not match:
                            raise RuntimeError("Failed to recognize SLC ID %s." % matched_ids[0])
                        mission = match.group('mission')
                        orbit_dict[matched_dt_orb] = fetch("%s.0" % matched_dts[0].isoformat(),
                                                           "%s.0" % matched_dts[-1].isoformat(),
                                                           mission=mission, dry_run=True)
                        if orbit_dict[matched_dt_orb] is None:
                            raise RuntimeError("Failed to query for an orbit URL for track {} {} {}.".format(track,
                                               matched_dts[0], matched_dts[-1]))

                    # get orbit type
                    orbit_type = 'poeorb'
                    for o in [orbit_dict[ref_dt_orb], orbit_dict[matched_dt_orb]]:
                        if RESORB_RE.search(o):
                            orbit_type = 'resorb'
                            break

                    # filter if we expect only precise orbits
                    if precise_orbit_only and orbit_type == 'resorb':
                        logger.info("Precise orbit required. Filtering job configured with restituted orbit.")
                    else:
                        # create jobs for backwards pair
                        if pre_ref_pd in ('backward', 'both'):
                            ifg_master_dt = all_dts[-1]
                            ifg_slave_dt = all_dts[0]
                            for swathnum in [1, 2, 3]:
                                stitched_args.append(False if len(ref_ids) == 1 or len(matched_ids) == 1 else True)
                                master_zip_urls.append([grouped_refs['hits'][i] for i in ref_ids])
                                master_orbit_urls.append(orbit_dict[ref_dt_orb])
                                slave_zip_urls.append([ref_scene['pre_matches']['hits'][i] for i in matched_ids])
                                slave_orbit_urls.append(orbit_dict[matched_dt_orb])
                                swathnums.append(swathnum)
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
                                    context['azimuth_looks'],
                                    context['range_looks'],
                                    context['filter_strength'],
                                    context.get('dem_type', 'SRTM+v3'),
                                ]).encode()).hexdigest()
                                ifg_ids.append(id_tmpl.format('M', len(ref_ids), len(matched_ids),
                                                              track, ifg_master_dt,
                                                              ifg_slave_dt, swathnum,
                                                              orbit_type, ifg_hash[0:4]))
                            
                        # create jobs for forward pair
                        if pre_ref_pd in ('forward', 'both'):
                            ifg_master_dt = all_dts[0]
                            ifg_slave_dt = all_dts[-1]
                            for swathnum in [1, 2, 3]:
                                stitched_args.append(False if len(ref_ids) == 1 or len(matched_ids) == 1 else True)
                                master_zip_urls.append([ref_scene['pre_matches']['hits'][i] for i in matched_ids])
                                master_orbit_urls.append(orbit_dict[matched_dt_orb])
                                slave_zip_urls.append([grouped_refs['hits'][i] for i in ref_ids])
                                slave_orbit_urls.append(orbit_dict[ref_dt_orb])
                                swathnums.append(swathnum)
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
                                    context['azimuth_looks'],
                                    context['range_looks'],
                                    context['filter_strength'],
                                    context.get('dem_type', 'SRTM+v3'),
                                ]).encode()).hexdigest()
                                ifg_ids.append(id_tmpl.format('S', len(matched_ids), len(ref_ids),
                                                              track, ifg_master_dt,
                                                              ifg_slave_dt, swathnum,
                                                              orbit_type, ifg_hash[0:4]))
                    
        # generate jobs for post-reference pairs
        if ref_scene['post_matches'] is not None:
            if track in ref_scene['post_matches']['grouped']:
                matched_days = ref_scene['post_matches']['grouped'][track]
                for matched_day, matched_ids in matched_days.items():
                    matched_dts = []
                    for i in matched_ids: matched_dts.extend(ref_scene['post_matches']['dates'][i])
                    #logger.info("post_matches matched_ids: %s" % matched_ids)
                    #logger.info("post_matches matched_dts: %s" % matched_dts)
                    all_dts = list(chain(ref_dts, matched_dts))
                    all_dts.sort()
                    
                    # set orbit urls and cache for matched dates
                    matched_dt_orb = "%s_%s" % (matched_dts[0].isoformat(), matched_dts[-1].isoformat())
                    if matched_dt_orb not in orbit_dict:
                        match = SLC_RE.search(matched_ids[0])
                        if not match:
                            raise RuntimeError("Failed to recognize SLC ID %s." % matched_ids[0])
                        mission = match.group('mission')
                        orbit_dict[matched_dt_orb] = fetch("%s.0" % matched_dts[0].isoformat(),
                                                           "%s.0" % matched_dts[-1].isoformat(),
                                                           mission=mission, dry_run=True)
                        if orbit_dict[matched_dt_orb] is None:
                            raise RuntimeError("Failed to query for an orbit URL for track {} {} {}.".format(track,
                                               matched_dts[0], matched_dts[-1]))

                    # get orbit type
                    orbit_type = 'poeorb'
                    for o in [orbit_dict[ref_dt_orb], orbit_dict[matched_dt_orb]]:
                        if RESORB_RE.search(o):
                            orbit_type = 'resorb'
                            break

                    # filter if we expect only precise orbits
                    if precise_orbit_only and orbit_type == 'resorb':
                        logger.info("Precise orbit required. Filtering job configured with restituted orbit.")
                    else:
                        # create jobs for backwards pair
                        if post_ref_pd in ('backward', 'both'):
                            ifg_master_dt = all_dts[-1]
                            ifg_slave_dt = all_dts[0]
                            for swathnum in [1, 2, 3]:
                                stitched_args.append(False if len(ref_ids) == 1 or len(matched_ids) == 1 else True)
                                master_zip_urls.append([ref_scene['post_matches']['hits'][i] for i in matched_ids])
                                master_orbit_urls.append(orbit_dict[matched_dt_orb])
                                slave_zip_urls.append([grouped_refs['hits'][i] for i in ref_ids])
                                slave_orbit_urls.append(orbit_dict[ref_dt_orb])
                                swathnums.append(swathnum)
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
                                    context['azimuth_looks'],
                                    context['range_looks'],
                                    context['filter_strength'],
                                    context.get('dem_type', 'SRTM+v3'),
                                ]).encode()).hexdigest()
                                ifg_ids.append(id_tmpl.format('S', len(matched_ids), len(ref_ids),
                                                              track, ifg_master_dt,
                                                              ifg_slave_dt, swathnum,
                                                              orbit_type, ifg_hash[0:4]))
                            
                        # create jobs for forward pair
                        if post_ref_pd in ('forward', 'both'):
                            ifg_master_dt = all_dts[0]
                            ifg_slave_dt = all_dts[-1]
                            for swathnum in [1, 2, 3]:
                                stitched_args.append(False if len(ref_ids) == 1 or len(matched_ids) == 1 else True)
                                master_zip_urls.append([grouped_refs['hits'][i] for i in ref_ids])
                                master_orbit_urls.append(orbit_dict[ref_dt_orb])
                                slave_zip_urls.append([ref_scene['post_matches']['hits'][i] for i in matched_ids])
                                slave_orbit_urls.append(orbit_dict[matched_dt_orb])
                                swathnums.append(swathnum)
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
                                    context['azimuth_looks'],
                                    context['range_looks'],
                                    context['filter_strength'],
                                    context.get('dem_type', 'SRTM+v3'),
                                ]).encode()).hexdigest()
                                ifg_ids.append(id_tmpl.format('M', len(ref_ids), len(matched_ids),
                                                              track, ifg_master_dt,
                                                              ifg_slave_dt, swathnum,
                                                              orbit_type, ifg_hash[0:4]))
                    
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
        cfgs = get_topsapp_cfgs(args.context_file, args.temporalBaseline)
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
