#!/usr/bin/env python
"""
Determine all combinations of stitch interferogram configurations"
"""

import os, sys, re, requests, json, logging, traceback, argparse, copy, bisect, shutil
from subprocess import check_call, CalledProcessError
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
from utils.queryBuilder import removeDuplicates


# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('enumerate_stitch_cfgs')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


BASE_PATH = os.path.dirname(__file__)


IFG_RE = re.compile(r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'-(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_')

ID_TMPL = "S1-IFG_STITCHED_TN{:03d}_{}-{}_s{}_{}-{}"


def call_noerr(cmd):
    """Run command and warn if exit status is not 0."""

    try: check_call(cmd, shell=True)
    except Exception as e:
        logger.warn("Got exception running {}: {}".format(cmd, str(e)))
        logger.warn("Traceback: {}".format(traceback.format_exc()))


def clean_query(query):
    """Clean list of ids from triggered query."""

    q = copy.deepcopy(query)
    and_filters = []
    for filt in q.get('query', {}).get('filtered', {}).get('filter', {}).get('and', []):
        if filt.get('ids', None) is not None: continue
        and_filters.append(filt)
    if len(and_filters) > 0:
        q['query']['filtered']['filter']['and'] = and_filters
    return q


def remove_partials(hits):
    """Return original metadata format removing partials."""

    retList = []
    for hit in hits:
        if 'fields' in hit and 'partial' in hit['fields']:
            for partial in hit['fields']['partial']:
                hit.setdefault('_source', {}).update(partial)
            del hit['fields']

        #url is not part of the metadata, so add it
        hit['_source']['metadata']['url'] = hit['_source']['urls'][0]
        retList.append(hit['_source']['metadata'])
        hit['_source']['metadata']['id'] = hit['_source']['id']
    return removeDuplicates(retList)


def get_date_pair_key(id):
    """Return date pair key and ifg pair key."""

    match = IFG_RE.search(id)
    if not match:
        raise RuntimeError("Failed to recognize IFG ID %s." % id)
    start_day_dt = datetime(int(match.group('start_year')),
                            int(match.group('start_month')),
                            int(match.group('start_day')),
                            0, 0, 0)
    end_day_dt = datetime(int(match.group('end_year')),
                          int(match.group('end_month')),
                          int(match.group('end_day')),
                          0, 0, 0)
    ifg_start_dt = datetime(int(match.group('start_year')),
                            int(match.group('start_month')),
                            int(match.group('start_day')),
                            int(match.group('start_hour')),
                            int(match.group('start_min')),
                            int(match.group('start_sec')))
    ifg_end_dt = datetime(int(match.group('end_year')),
                          int(match.group('end_month')),
                          int(match.group('end_day')),
                          int(match.group('end_hour')),
                          int(match.group('end_min')),
                          int(match.group('end_sec')))
    dt_pair_key = "%s_%s" % (start_day_dt.strftime('%Y%m%d'),
                             end_day_dt.strftime('%Y%m%d'))
    ifg_pair_key = "%s_%s" % (ifg_start_dt.strftime('%Y%m%dT%H%M%S'),
                              ifg_end_dt.strftime('%Y%m%dT%H%M%S'))
    return dt_pair_key, ifg_pair_key


def group_frames_by_track_date(frames):
    """Classify frames by track and date."""

    hits = {}
    grouped = {}
    dates = {}
    footprints = {}
    metadata = {}
    dedup_ifgs = {}
    for h in frames: 
        if h['_id'] in hits: continue
        fields = h['fields']['partial'][0]
        track_number = fields['metadata']['trackNumber']

        # handle differences in metadata formats prior to v1.2.1
        if isinstance(fields['metadata']['swath'], list):
            swath = fields['metadata']['swath'][0]
        else:
            swath = fields['metadata']['swath']

        # get product url; prefer S3
        prod_url = fields['urls'][0]
        if len(fields['urls']) > 1:
            for u in fields['urls']:
                if u.startswith('s3://'):
                    prod_url = u
                    break

        hits[h['_id']] = prod_url
        dt_pair_key, ifg_pair_key = get_date_pair_key(h['_id'])
        dedup_key = "%s_%s_%s" % (track_number, ifg_pair_key, swath)
        if dedup_key in dedup_ifgs:
            dedup_ifgs[dedup_key].append(h['_id'])
            continue
        else: dedup_ifgs[dedup_key] = []
        bisect.insort(grouped.setdefault(track_number, {}) \
                             .setdefault(dt_pair_key, {}) \
                             .setdefault(swath, []),
                      (ifg_pair_key, h['_id']))
        dates[h['_id']] = dt_pair_key
        footprints[h['_id']] = fields['location']
        metadata[h['_id']] = fields['metadata']
    return {
        "hits": hits,
        "grouped": grouped,
        "dates": dates,
        "footprints": footprints,
        "metadata": metadata,
        "deduped": dedup_ifgs,
    }


def get_stitch_cfgs(context_file):
    """Return all possible stitch interferogram configurations."""

    # get context
    with open(context_file) as f:
        context = json.load(f)

    # get args
    project = context['project']
    direction = context.get('direction', 'along')
    subswaths = [int(i) for i in context.get('subswaths', "1 2 3").split()]
    subswaths.sort()
    min_stitch_count = int(context['min_stitch_count'])
    extra_products = [i.strip() for i in context.get('extra_products', 'los.rdr.geo').split()]
    orig_query = context['query']
    logger.info("orig_query: %s" % json.dumps(orig_query, indent=2))

    # cleanse query of ids from triggered rules
    query = clean_query(orig_query)
    logger.info("clean query: %s" % json.dumps(query, indent=2))

    # log enumerator params
    logger.info("project: %s" % project)
    logger.info("direction: %s" % direction)
    logger.info("subswaths: %s" % subswaths)
    logger.info("min_stitch_count: %s" % min_stitch_count)
    logger.info("extra_products: %s" % extra_products)

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
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])

    # remove partial fields and reformat metadata as expected by stitcher_utils
    #hits = remove_partials(hits)

    # extract reference ids
    ids = { h['_id']: True for h in hits }
    logger.info("ids: {}".format(json.dumps(ids, indent=2)))
    logger.info("hits count: {}".format(len(hits)))

    # dump metadata
    valid_meta_ts_out_file = "valid_meta_ts_out.json"
    with open(valid_meta_ts_out_file, 'w') as f:
        json.dump(hits, f, indent=2)

    # group frames by track and date pairs
    grouped = group_frames_by_track_date(hits)
    logger.info("grouped: %s" % json.dumps(grouped, indent=2))

    # enumerate configs
    projects = []
    directions = []
    extra_products_list = []
    filenames = []
    filename_urls = []
    ifg_ids = []
    base_products = ['filt_topophase.unw.geo', 'filt_topophase.unw.conncomp.geo', 'phsig.cor.geo']
    base_products.extend(extra_products)
    for track in sorted(grouped['grouped']):
        for dt_pair in sorted(grouped['grouped'][track]):
            stitch_count = 0

            # filter scenes without all requested subswaths
            swath_check = {}
            for swath in subswaths:
                if swath not in grouped['grouped'][track][dt_pair]:
                    logger.warning("Did not find singlescene IFGs for subswath %s for track %s dates %s. "
                                   "Inteferogram job for subswath may have failed. "
                                   "Putting dummy ids for missing subswath." % (swath, track, dt_pair))
                    grouped['grouped'][track][dt_pair][swath] = []

                for tr, id in grouped['grouped'][track][dt_pair][swath]:
                    swath_check.setdefault(tr, {})[swath] = id
            skip_tr = {}
            for tr in sorted(swath_check):
                for swath in subswaths:
                    if swath not in swath_check[tr]: skip_tr[tr] = True
                
            furls = []
            swathnums = []
            ifg_sts = set()
            ifg_ets = set()
            fnames_tr = {}

            for swath in subswaths:
                swathnums.append(swath)
                for tr, id in grouped['grouped'][track][dt_pair][swath]:
                    if tr in skip_tr:
                        logger.warning("Skipping %s for scene %s since only subswaths %s exist." %
                                       (id, tr, sorted(swath_check[tr].keys())))
                        continue
                    bisect.insort(fnames_tr.setdefault(tr, []),
                                  os.path.join(id, 'merged', 'filt_topophase.unw.geo'))
                    for prod_file in base_products:
                        furls.append({
                            'url': "%s/merged/%s" % (grouped['hits'][id], prod_file),
                            'local_path': "%s/merged/" % id,
                        })
                        furls.append({
                            'url': "%s/merged/%s.xml" % (grouped['hits'][id], prod_file),
                            'local_path': "%s/merged/" % id,
                        })
                    furls.append({
                        'url': "%s/fine_interferogram.xml" % grouped['hits'][id],
                        'local_path': "%s/" % id,
                    })
                    furls.append({
                        'url': "%s/%s.dataset.json" % (grouped['hits'][id], id),
                        'local_path': "%s/_%s.dataset.json" % (id, id),
                    })
                    furls.append({
                        'url': "%s/%s.met.json" % (grouped['hits'][id], id),
                        'local_path': "%s/_%s.met.json" % (id, id),
                    })
                    stitch_count += 1
                    st, et = tr.split('_')
                    ifg_sts.add(st)
                    ifg_ets.add(et)
            ifg_sts = list(ifg_sts)
            ifg_sts.sort()
            ifg_ets = list(ifg_ets)
            ifg_ets.sort()

            # check minimum stitch count met
            if stitch_count < min_stitch_count:
                logger.warning("Failed to find minimum stitch count of %s for track %s date pair %s: %s" %
                               (min_stitch_count, track, dt_pair, stitch_count))
                continue

            # build job params
            projects.append(project)
            directions.append(direction)
            extra_products_list.append(extra_products)
            filenames.append([fnames_tr[tr] for tr in sorted(fnames_tr)])
            filename_urls.append(furls)
            ifg_hash = hashlib.md5(json.dumps([
                projects[-1],
                directions[-1],
                extra_products_list[-1],
                filenames[-1],
                filename_urls[-1],
            ], sort_keys=True)).hexdigest()
            ifg_ids.append(ID_TMPL.format(int(track), ifg_sts[0], ifg_ets[-1], 
                           ''.join(map(str, swathnums)), direction, ifg_hash[0:4]))
    logger.info("projects: %s" % projects)
    logger.info("directions: %s" % directions)
    logger.info("extra_products: %s" % extra_products_list)
    logger.info("filenames: %s" % json.dumps(filenames, indent=2))
    logger.info("filename_urls: %s" % json.dumps(filename_urls, indent=2))
    logger.info("ifg_ids: %s" % ifg_ids)
    return ( projects, directions, extra_products_list, filenames, filename_urls, ifg_ids )


def get_validated_cfgs(context_file):
    """Return all possible stitch interferogram configurations for validated stack."""

    # get context
    with open(context_file) as f:
        context = json.load(f)
    work_dir = os.path.dirname(os.path.abspath(context_file))

    # get args
    project = context['project']
    direction = context.get('direction', 'along')
    extra_products = [i.strip() for i in context.get('extra_products', 'los.rdr.geo').split()]
    prods = context['path']
    orig_query = context['query']
    logger.info("orig_query: %s" % json.dumps(orig_query, indent=2))

    # cleanse query of ids from triggered rules
    query = clean_query(orig_query)
    logger.info("clean query: %s" % json.dumps(query, indent=2))

    # log enumerator params
    logger.info("project: %s" % project)
    logger.info("direction: %s" % direction)
    logger.info("extra_products: %s" % extra_products)

    # enumerate configs
    projects = []
    directions = []
    extra_products_list = []
    filenames = []
    filename_urls = []
    ifg_ids = []
    base_products = ['filt_topophase.unw.geo', 'filt_topophase.unw.conncomp.geo', 'phsig.cor.geo']
    base_products.extend(extra_products)
    logger.info(os.getcwd())
    for prod in prods:
        valid_ifg_in = os.path.join(work_dir, prod, 'valid_ifg_in.json')
        valid_ts_in = os.path.join(work_dir, prod, 'valid_ts_in.json')
        if os.path.exists(valid_ifg_in):
            valid_in = valid_ifg_in
        elif os.path.exists(valid_ts_in):
            valid_in = valid_ts_in
        else:
            logger.info("Couldn't find {} or {}. Skipping {}.".format(valid_ifg_in, valid_ts_in, prod))
            continue
        with open(valid_in) as f:
            valid_in_info = json.load(f)
        valid_out = os.path.join(work_dir, prod, valid_in_info['output_file'])
        with open(valid_out) as f:
            valid_out_info = json.load(f)
        urls_list = valid_out_info.get('urls_list', [])
        for stitch_list in urls_list:
            projects.append(project)
            directions.append(direction)
            extra_products_list.append(extra_products)
            furls = []
            swathnums = []
            ifg_sts = []
            ifg_ets = []
            fnames_tr = {}
            for swath in sorted(stitch_list):
                swathnums.append(swath)
                for prod_url in sorted(stitch_list[swath],
                                       key=lambda x: get_date_pair_key(os.path.basename(x))[1]):
                    id = os.path.basename(prod_url)
                    tr = get_date_pair_key(os.path.basename(id))[1]
                    bisect.insort(fnames_tr.setdefault(tr, []),
                                  os.path.join(id, 'merged', 'filt_topophase.unw.geo'))
                    for prod_file in base_products:
                        furls.append({
                            'url': "%s/merged/%s" % (prod_url, prod_file),
                            'local_path': "%s/merged/" % id,
                        })
                        furls.append({
                            'url': "%s/merged/%s.xml" % (prod_url, prod_file),
                            'local_path': "%s/merged/" % id,
                        })
                    furls.append({
                        'url': "%s/fine_interferogram.xml" % prod_url,
                        'local_path': "%s/" % id,
                    })
                    furls.append({
                        'url': "%s/%s.dataset.json" % (prod_url, id),
                        'local_path': "%s/_%s.dataset.json" % (id, id),
                    })
                    furls.append({
                        'url': "%s/%s.met.json" % (prod_url, id),
                        'local_path': "%s/_%s.met.json" % (id, id),
                    })
                    st, et = re.search(r'(\d{8}T\d{6})-(\d{8}T\d{6})', id).groups()
                    ifg_sts.append(st)
                    ifg_ets.append(et)
            ifg_sts.sort()
            ifg_ets.sort()
            filenames.append([fnames_tr[tr] for tr in sorted(fnames_tr)])
            filename_urls.append(furls)
            ifg_hash = hashlib.md5(json.dumps([
                projects[-1],
                directions[-1],
                extra_products_list[-1],
                filenames[-1],
                filename_urls[-1],
            ], sort_keys=True)).hexdigest()
            ifg_ids.append(ID_TMPL.format(int(valid_in_info['track']), ifg_sts[0], ifg_ets[-1], 
                           ''.join(map(str, swathnums)), direction, ifg_hash[0:4]))
    logger.info("projects: %s" % projects)
    logger.info("directions: %s" % directions)
    logger.info("extra_products: %s" % extra_products_list)
    logger.info("filenames: %s" % json.dumps(filenames, indent=2))
    logger.info("filename_urls: %s" % json.dumps(filename_urls, indent=2))
    logger.info("ifg_ids: %s" % ifg_ids)
    for prod in prods: shutil.rmtree(os.path.join(work_dir, prod)) # delete validated products
    return ( projects, directions, extra_products_list, filenames, filename_urls, ifg_ids )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("context_file", help="context file")
    args = parser.parse_args()
    ifg_id_dict = {}
    try:
        cfgs = get_stitch_cfgs(args.context_file)
        print("Enumerated %d cfgs:" % len(cfgs[0]))
        for i in range(len(cfgs[0])):
            print("#" * 80)
            print("project: %s" % cfgs[0][i])
            print("direction: %s" % cfgs[1][i])
            print("extra_products: %s" % cfgs[2][i])
            print("filename: %s" % cfgs[3][i])
            print("filename_urls: %s" % cfgs[4][i])
            print("ifg_id: %s" % cfgs[5][i])
            if cfgs[5][i] in ifg_id_dict: raise RuntimeError("ifg %s already found." % cfgs[5][i])
            ifg_id_dict[cfgs[5][i]] = True
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("{}\n".format(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("{}\n".format(traceback.format_exc()))
        raise
