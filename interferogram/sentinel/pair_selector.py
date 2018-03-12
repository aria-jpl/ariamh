#!/usr/bin/env python3
"""
Determine last matching temporal S1 pair using parameters
from an S1 swath metadata.  
"""

import os, sys, re, requests, json, traceback, argparse
from datetime import datetime, timedelta
import numpy as np
from osgeo import ogr, osr
from requests.packages.urllib3.exceptions import (InsecureRequestWarning,
                                                  InsecurePlatformWarning)

import isce
from utils.UrlUtils import UrlUtils as UU

from fetchOrbit import fetch


ID_RE = re.compile(r'^s1\w-iw(\d)-.*?-(.*?)-(\d{4})(\d{2})(\d{2})t(\d{2})(\d{2})(\d{2})-')

RESORB_RE = re.compile(r'_RESORB_')


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
    print("geom1: %s" % geom1)
    area1 = geom1.GetArea() # in square meters
    print("area (m^2) for geom1: %s" % area1)
    
    # get area of second geometry
    geom2 = ogr.CreateGeometryFromJson(json.dumps(loc2))
    geom2.Transform(transform)
    print("geom2: %s" % geom2)
    area2 = geom2.GetArea() # in square meters
    print("area (m^2) for geom2: %s" % area2)
    
    # get area of intersection
    intersection = geom1.Intersection(geom2)
    intersection.Transform(transform)
    print("intersection: %s" % intersection)
    intersection_area = intersection.GetArea() # in square meters
    print("area (m^2) for intersection: %s" % intersection_area)
    if area1 > area2:
        return intersection_area/area1
    else:
        return intersection_area/area2
    

def pair_selector(id, margin=0.2, overlap_min=.5, frame_id_margin=3):
    """Return last matching temporal S1 pair."""

    uu = UU()
    print("S1 ID is {}".format(id))
    print("rest_url: {}".format(uu.rest_url))
    print("dav_url: {}".format(uu.dav_url))
    print("version: {}".format(uu.version))
    print("grq_index_prefix: {}".format(uu.grq_index_prefix))

    # extract info
    match = ID_RE.search(id)
    if match is None:
        raise RuntimeError("Swath number extraction error: {}.".format(id))
    swath_num = int(match.group(1))
    vtype = match.group(2)
    yr = int(match.group(3))
    mo = int(match.group(4))
    dy = int(match.group(5))
    hr = int(match.group(6))
    mn = int(match.group(7))
    ss = int(match.group(8))

    # get index name and url
    idx = "{}_{}_s1-swath".format(uu.grq_index_prefix,
                                  uu.version.replace('.', ''))
    url = "{}{}/_search".format(uu.rest_url, idx)
    print("idx: {}".format(idx))
    print("url: {}".format(url))

    # get metadata
    query = {
        "query": {
            "term": {
                "_id": id
            }
        },
        "partial_fields" : {
            "partial" : {
                "exclude" : "city",
            }
        }
    }
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    res = r.json()
    if res['hits']['total'] != 1:
        raise RuntimeError("Failed to find exactly 1 result for {}:\n\n{}".format(id, json.dumps(res, indent=2)))
    hit = res['hits']['hits'][0]['fields']['partial'][0]
    #print(json.dumps(hit, indent=2))

    # find matching ones within +-50 days
    sensingStart = datetime.strptime(hit['metadata']['sensingStart'], '%Y-%m-%dT%H:%M:%S.%f')
    query_start = (sensingStart - timedelta(days=50)).isoformat()
    query_stop = (sensingStart + timedelta(days=50)).isoformat()
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "system_version": hit['system_version']
                        }
                    }, 
                    {
                        "term": {
                            "metadata.trackNumber": hit['metadata']['trackNumber']
                        }
                    }, 
                    {
                        "range": {
                            "metadata.frameID": {
                                "from": int(hit['metadata']['frameID']) - frame_id_margin,
                                "to": int(hit['metadata']['frameID']) + frame_id_margin
                            }
                        }
                    }, 
                    {
                        "bool": {
                            "should": [
                                {
                                    "range": {
                                        "metadata.sensingStart": {
                                            "from": query_start,
                                            "to": query_stop
                                        }
                                    }
                                }, 
                                {
                                    "range": {
                                        "metadata.sensingStop": {
                                            "from": query_start,
                                            "to": query_stop
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
                    "order": "desc"
                }
            }
        ], 
        "partial_fields" : {
            "partial" : {
                "exclude" : "city",
            }
        }
    }
    #print(json.dumps(query, indent=2))
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    res = r.json()
    print("total matches: {}".format(res['hits']['total']))
    matches = res['hits']['hits']
    print("matches: {}".format([m['_id'] for m in matches]))

    # filter matches
    filtered_matches = []
    for m in matches:
        h = m['fields']['partial'][0]
        #print("h: {}".format(json.dumps(h, indent=2)))
        if h['id'] == id:
            print("Filtering self: %s" % h['id'])
            continue
        match = ID_RE.search(h['id'])
        if match is None:
            print("Filtering unrecognized id: %s" % h['id'])
            continue
        sn = int(match.group(1))
        if sn != swath_num:
            print("Filtering %s due to unmatched swath number. Got %s but should be %s." % (h['id'], sn, swath_num))
            continue
        vt = match.group(2)
        if vt != vtype:
            print("Filtering %s due to unmatched vtype. Got %s but should be %s." % (h['id'], vt, vtype))
            continue
        overlap_pct = get_overlap(hit['location'], h['location'])
        print("overlap_pct is: %s" % overlap_pct)
        if overlap_pct < overlap_min:
            print("Filtering %s since overlap_pct < min overlap threshold of %s." % (h['id'], overlap_min))
            continue
        filtered_matches.append(h)
    print("total filtered_matches: {}".format(len(filtered_matches)))
    print("filtered_matches: {}".format([fm['id'] for fm in filtered_matches]))

    # return if no filtered matches
    if len(filtered_matches) == 0:
        return filtered_matches

    # get bbox arg
    bbox = np.array(hit['metadata']['bbox'])
    bbox_str = "{0:.2f} {1:.2f} {2:.2f} {3:.2f}".format(
        bbox[:,0].min() - margin,
        bbox[:,0].max() + margin,
        bbox[:,1].min() - margin,
        bbox[:,1].max() + margin)

    # get orbit URL
    orbit_url = fetch(hit['starttime'], hit['endtime'], dry_run=True)
    if orbit_url is None:
        raise RuntimeError("Failed to query for an orbit URL for {}.".format(
                           os.path.basename(hit['metadata']['archive_url'])))

    # result json
    ret_list = []
    for filtered_match in filtered_matches:
        j = {
            "swath": swath_num,
            "bbox_str": bbox_str,
            "id": [ id ],
            "bbox": [ hit['metadata']['bbox'] ],
            "archive_url": [ hit['metadata']['archive_url'] ],
            "frameID": [ hit['metadata']['frameID'] ],
            "trackNumber": [ hit['metadata']['trackNumber'] ],
            "orbit_url": [ orbit_url ],
        }
        #print("filtered match: {}".format(json.dumps(filtered_match, indent=2)))
        st_time = datetime.strptime(filtered_match['metadata']['sensingStart'], '%Y-%m-%dT%H:%M:%S.%f')

        # extract info
        match = ID_RE.search(filtered_match['id'])
        if match is None:
            raise RuntimeError("Swath number extraction error: {}.".format(filtered_match['id']))
        match_swath_num = int(match.group(1))
        match_vtype = match.group(2)
        match_yr = int(match.group(3))
        match_mo = int(match.group(4))
        match_dy = int(match.group(5))
        match_hr = int(match.group(6))
        match_mn = int(match.group(7))
        match_ss = int(match.group(8))

        # get orbit URL
        match_orbit_url = fetch(filtered_match['starttime'], filtered_match['endtime'], dry_run=True)
        if match_orbit_url is None:
            raise RuntimeError("Failed to query for an orbit URL for {}.".format(
                               os.path.basename(filtered_match['metadata']['archive_url'])))

        # each pair is (master, slave); determine which is which
        if st_time > sensingStart:
            ifg_start_dt = datetime(yr, mo, dy, hr, mn, ss)
            ifg_end_dt = datetime(match_yr, match_mo, match_dy, match_hr, match_mn, match_ss)
            j['id'].append(filtered_match['id'])
            j['bbox'].append(filtered_match['metadata']['bbox'])
            j['archive_url'].append(filtered_match['metadata']['archive_url'])
            j['frameID'].append(filtered_match['metadata']['frameID'])
            j['trackNumber'].append(filtered_match['metadata']['trackNumber'])
            j['orbit_url'].append(match_orbit_url)
        else:
            ifg_start_dt = datetime(match_yr, match_mo, match_dy, match_hr, match_mn, match_ss)
            ifg_end_dt = datetime(yr, mo, dy, hr, mn, ss)
            j['id'].insert(0, filtered_match['id'])
            j['bbox'].insert(0, filtered_match['metadata']['bbox'])
            j['archive_url'].insert(0, filtered_match['metadata']['archive_url'])
            j['frameID'].insert(0, filtered_match['metadata']['frameID'])
            j['trackNumber'].insert(0, filtered_match['metadata']['trackNumber'])
            j['orbit_url'].insert(0, match_orbit_url)

        # get ifg orbit type
        orbit_type = 'poeorb'
        for u in j['orbit_url']:
            if RESORB_RE.search(u):
                orbit_type = 'resorb'
                break
        j['orbit_type'] = orbit_type

        # generate ifg id
        ifg_id_tmpl = "S1-IFG_FID{:03d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s{}-{}"
        j['ifg_id'] = ifg_id_tmpl.format(
            filtered_match['metadata']['frameID'],
            filtered_match['metadata']['trackNumber'],
            ifg_start_dt, ifg_end_dt, swath_num, orbit_type)

        # append
        ret_list.append(j)

    # write out pair info
    with open('pair.json', 'w') as f:
        json.dump({'pairs': ret_list}, f, indent=2, sort_keys=True)

    return ret_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("id", help="S1 product ID")
    parser.add_argument("-m", "--margin", dest="margin", type=float,
                        default=0.2, help="bounding box margin")
    parser.add_argument("-o", "--overlap-minimum", dest="overlap_min",
                        type=float, default=0.5,
                        help="minimum percent overlap [0. -> 1.]")
    parser.add_argument("-f", "--frame_id-margin", dest="frame_id_margin",
                        type=int, default=3,
                        help="frame ID margin")
    args = parser.parse_args()
    try: pair_selector(args.id, args.margin, args.overlap_min, args.frame_id_margin)
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("{}\n".format(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("{}\n".format(traceback.format_exc()))
        raise
