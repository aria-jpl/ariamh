from builtins import str
import os, sys, re, requests, json, logging, traceback, argparse, copy, bisect
import hashlib
from itertools import product, chain
from datetime import datetime, timedelta
#from hysds.celery import app
from utils.UrlUtils import UrlUtils as UU
from fetchOrbitES import fetch
from datetime import datetime

# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('enumerate_acquisations')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


RESORB_RE = re.compile(r'_RESORB_')

SLC_RE = re.compile(r'(?P<mission>S1\w)_IW_SLC__.*?' +
                    r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'_(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_.*$')

IFG_ID_TMPL = "S1-IFG_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s123-{}-{}-standard_product"
RSP_ID_TMPL = "S1-SLCP_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s{}-{}-{}"


MOZART_ES_ENDPOINT = "MOZART"
GRQ_ES_ENDPOINT = "GRQ"

def query_grq( doc_id):
    """
    This function queries ES
    :param endpoint: the value specifies which ES endpoint to send query
     can be MOZART or GRQ
    :param doc_id: id of product or job
    :return: result from elasticsearch
    """
    es_url, es_index = None, None

    '''
    if endpoint == GRQ_ES_ENDPOINT:
        es_url = app.conf["GRQ_ES_URL"]
        es_index = "grq"
    if endpoint == MOZART_ES_ENDPOINT:
        es_url = app.conf['JOBS_ES_URL']
        es_index = "job_status-current"
    '''

    uu = UU()
    logger.info("rest_url: {}".format(uu.rest_url))
    logger.info("grq_index_prefix: {}".format(uu.grq_index_prefix))

    # get normalized rest url
    es_url = uu.rest_url[:-1] if uu.rest_url.endswith('/') else uu.rest_url
    es_index = uu.grq_index_prefix

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": doc_id}} # add job status:
                ]
            }
        }
    }
    #print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits']
'''
def get_dem_type(slc_source):
    dem_type = "SRTM+v3"
    if slc_source['city'] is not None and len(slc_source['city'])>0:
	if slc_source['city'][0]['country_name'] is not None and slc_source['city'][0]['country_name'].lower() == "united states":
	    dem_type="Ned1"
    return dem_type
'''

def get_dem_type(info):
    """Get dem type."""

    dem_type = "SRTM+v3"

    dems = {}
    for id in info:
	dem_type = "SRTM+v3"
        h = info[id]
        fields = h["_source"]
	try:
	    if 'city' in fields:
	        if fields['city'][0]['country_name'] is not None and fields['city'][0]['country_name'].lower() == "united states":
                    dem_type="Ned1"
                dems.setdefault(dem_type, []).append(id)
	except:
	    dem_type = "SRTM+v3"

    if len(dems) != 1:
	logger.info("There are more than one type of dem, so selecting SRTM+v3")
	dem_type = "SRTM+v3"
    return dem_type



def print_list(l):
    for f in l:
	print("\n%s"%f)

def get_metadata(id, rest_url, url):
    """Get SLC metadata."""

    # query hits
    query = {
        "query": {
            "term": {
                "_id": id
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

def convert_number(x):

    x = float(x)
    data = ''
    y = abs(x)
    pre_y = str(y).split('.')[0]
    if int(pre_y)>99:
        pre_y = pre_y[:2]
    else:
        pre_y = pre_y.rjust(2, '0')

    post_y = '000'
    post_y = str(y).split('.')[1]
        
    if int(post_y)>999:
        post_y = post_y[:3]
    else:
        post_y = post_y.ljust(3, '0')
        
    print("post_y : %s " %post_y)

    if x<0:
        data = "{}{}S".format(pre_y, post_y)
    else:
        data = "{}{}N".format(pre_y, post_y)

    return data


def get_minmax(geojson):
    '''returns the minmax tuple of a geojson'''
    lats = [x[1] for x in geojson['coordinates'][0]]
    return min(lats), max(lats)

def lat_convert(value):
    '''convert floating point value into proper string'''
    return '{}{}'.format(str(abs(int(value * 100))).zfill(4),'N' if value > 0 else 'S')

def get_minmax_s(geojson):
    '''returns the minmax tuple of a geojson'''
    return (min(geojson['coordinates'][0], key=lambda x: x[1])[1], max(geojson['coordinates'][0], key=lambda x: x[1])[1])


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
        fields = h['_source']
        prod_url = fields['urls'][0]
        if len(fields['urls']) > 1:
            for u in fields['urls']:
                if u.startswith('s3://'):
                    prod_url = u
                    break
        urls.append("%s/%s" % (prod_url, fields['metadata']['archive_filename']))
    return urls


def get_bool_param(ctx, param):
    """Return bool param from context."""

    if param in ctx and isinstance(ctx[param], bool): return ctx[param]
    return True if ctx.get(param, 'true').strip().lower() == 'true' else False

def get_track(info):
    """Get track number."""

    tracks = {}
    for id in info:
        h = info[id]
        fields = h["_source"]
        track = fields['metadata']['track_number']
        tracks.setdefault(track, []).append(id)
    if len(tracks) != 1:
        raise RuntimeError("Failed to find SLCs for only 1 track.")
    return track

def initiate_standard_product_job(context_file):
    # get context
    with open(context_file) as f:
        context = json.load(f)


    input_metadata = context['input_metadata']
    if type(input_metadata) is list:
        input_metadata = input_metadata[0]

    # get args
    project = input_metadata['project']
    if type(project) is list:
        project = project[0]

    #master_ids = [i.strip() for i in input_metadata['master_ids'].split()]
    #slave_ids = [i.strip() for i in input_metadata['slave_ids'].split()]
    master_scenes = input_metadata["master_scenes"]
    slave_scenes = input_metadata["slave_scenes"]
    master_ids = input_metadata["master_slcs"]
    slave_ids = input_metadata["slave_slcs"]
    union_geojson = input_metadata["union_geojson"]
    direction = input_metadata["direction"]
    platform = input_metadata["platform"]
    subswaths = [1, 2, 3] #context['subswaths']
    
    azimuth_looks = 7
    if 'azimuth_looks' in input_metadata:
	azimuth_looks = int(input_metadata['azimuth_looks'])

    range_looks = 19
    if 'range_looks' in input_metadata:
        range_looks = int(input_metadata['range_looks'])

    filter_strength = 0.5
    if 'filter_strength' in input_metadata:
        filter_strength = float(input_metadata['filter_strength'])

    precise_orbit_only = True
    if 'precise_orbit_only' in input_metadata:
	precise_orbit_only = get_bool_param(input_metadata, 'precise_orbit_only')

    job_priority = int(input_metadata['priority'])

    subswaths = [1, 2, 3]


    # log inputs
    logger.info("project: {}".format(project))
    logger.info("master_scenes: {}".format(master_scenes))
    logger.info("slave_scenes: {}".format(slave_scenes))
    logger.info("master_ids: {}".format(master_ids))
    logger.info("slave_ids: {}".format(slave_ids))
    logger.info("subswaths: {}".format(subswaths))
    logger.info("azimuth_looks: {}".format(azimuth_looks))
    logger.info("range_looks: {}".format(range_looks))
    logger.info("filter_strength: {}".format(filter_strength))
    logger.info("precise_orbit_only: {}".format(precise_orbit_only))
    logger.info("direction : {}".format(direction))
    logger.info("platform : {}".format(platform))

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

    ref_scence = master_md
    if len(master_ids)==1:
	ref_scence = master_md
    elif len(slave_ids)==1:
	ref_scence = slave_md
    elif len(master_ids) > 1 and  len(slave_ids)>1:
	raise RuntimeError("Single Scene Reference Required.")
 
    # get urls (prefer s3)
    master_urls = get_urls(master_md) 
    logger.info("master_urls: {}".format(master_urls))
    slave_urls = get_urls(slave_md) 
    logger.info("slave_ids: {}".format(slave_urls))

    dem_type = get_dem_type(master_md)

    # get dem_type
    dem_type = get_dem_type(master_md)
    logger.info("master_dem_type: {}".format(dem_type))
    slave_dem_type = get_dem_type(slave_md)
    logger.info("slave_dem_type: {}".format(slave_dem_type))
    if dem_type != slave_dem_type:
	dem_type = "SRTM+v3"


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
    #if precise_orbit_only and orbit_type == 'resorb':
        #raise RuntimeError("Precise orbit required.")


    '''
    cord = union_geojson["coordinates"][0]
    polygon = Polygon(cord)
    x = polygon.bounds
    west_lat1 = convert_number(x[1])
    west_lat2 = convert_number(x[3])
    '''
    minlat, maxlat = get_minmax(union_geojson)
    west_lat= "{}_{}".format(convert_number(minlat), convert_number(maxlat))
    logger.info("west_latitude : {}".format(west_lat))

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
    dem_types = []
    job_priorities =[]
    orbit_dict = {}
    master_scenes = []
    slave_scenes = []
    union_geojsons = []
    union_geojsons.append(union_geojson)
    west_lats = []
    west_lats.append(west_lat)   
    ifg_hashes = [] 
    directions = []
    directions.append(direction)
    platforms = []
    platforms.append(platform)
    tracks = []
    tracks.append(track)
    orbit_types = []
    orbit_types.append(orbit_type)

    # generate job configs
    bbox = [-90., 90., -180., 180.]
    auto_bbox = True
    id_tmpl = IFG_ID_TMPL

    stitched_args.append(False if len(master_ids) == 1 or len(slave_ids) == 1 else True)
    master_zip_urls.append(master_urls)
    master_orbit_urls.append(master_orbit_url)
    slave_zip_urls.append(slave_urls)
    slave_orbit_urls.append(slave_orbit_url)
    swathnums.append(subswaths)
    bboxes.append(bbox)
    auto_bboxes.append(auto_bbox)
    projects.append(project)
    dem_types.append(dem_type)
    job_priorities.append(job_priority)
    master_scenes.append(master_ids)
    slave_scenes.append(slave_ids)
    ifg_master_dts = []
    ifg_master_dts.append(ifg_master_dt.strftime("%Y%m%d"))
    ifg_slave_dts = []
    ifg_slave_dts.append(ifg_slave_dt.strftime("%Y%m%d"))

    satelite_orientation = "D"
    if direction.lower()=="asc":
        satelite_orientation = "A"
    satelite_look_direction = "R"
        

    ifg_hash = hashlib.md5(json.dumps([
        id_tmpl,
        stitched_args[-1],
        master_zip_urls[-1],
        master_orbit_urls[-1],
        slave_zip_urls[-1],
        slave_orbit_urls[-1],
        #swathnums[-1],
        #bboxes[-1],
        #auto_bboxes[-1],
        projects[-1],
        #azimuth_looks,
        track,
        filter_strength,
	dem_type
    ])).hexdigest()
    ifg_hashes.append(ifg_hash[0:4])

    ifg_ids.append(id_tmpl.format('M', len(master_ids), len(slave_ids),
                                      track, ifg_master_dt,
                                      ifg_slave_dt, orbit_type, ifg_hash[0:4]))
                            

    logger.info("\n\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n" %(projects, stitched_args, auto_bboxes, ifg_ids, master_zip_urls, master_orbit_urls, slave_zip_urls, slave_orbit_urls, swathnums, bboxes, dem_types, union_geojsons, ifg_hashes, platforms, directions, west_lats, tracks, orbit_types, ifg_master_dts, ifg_slave_dts))
    return ( projects, stitched_args, auto_bboxes, ifg_ids, master_zip_urls,
             master_orbit_urls, slave_zip_urls, slave_orbit_urls, swathnums,
             bboxes, dem_types, job_priorities, master_scenes,slave_scenes, union_geojsons, ifg_hashes, platforms, directions, west_lats, tracks, orbit_types, ifg_master_dts, ifg_slave_dts)

'''
def initiate_sp2(context_file):

    # get context
    with open(context_file) as f:
        context = json.load(f)

    ifg_ids = []
    master_zip_urls = []
    master_orbit_urls = []
    slave_zip_urls = []
    slave_orbit_urls = []
    swathnums = [1, 2, 3]
    bboxes = []
    auto_bboxes = []
    orbit_dict = {}
    dem_type = "SRTM+v3"
    master_orbit_number = None
    slave_orbit_number = None
    #bboxes.append(bbox)
    #auto_bboxes.append(auto_bbox)
    projects.append(context["project"])


    #master_slcs = context["master_slc"]
    #slave_slcs = context["slave_slcs"]
    
    master_slcs = ["acquisition-S1A_IW_SLC__1SDV_20180807T135955_20180807T140022_023141_02837E_DA79"]
    slave_slcs =["acquisition-S1A_IW_SLC__1SDV_20180714T140019_20180714T140046_022791_027880_AFD3", "acquisition-S1A_IW_SLC__1SDV_20180714T135954_20180714T140021_022791_027880_D224", "acquisition-S1A_IW_SLC__1SDV_20180714T135929_20180714T135956_022791_027880_9FCA"]

    #print(master_slcs)

    print("Processing Master")
    for slc_id in master_slcs:
  	result = query_grq(slc_id)[0]['_source']
	track = result['metadata']['track_number']
	master_orbit_number = result['metadata']['orbitNumber']
        zip_url = get_prod_url(result['urls'], result['metadata']['archive_filename'])
	orbit_url = get_orbit_url (slc_id, track)
        dem_type = get_dem_type(result)
        print("%s : %s : %s : %s : %s : %s" %(master_orbit_number, slc_id, track, zip_url, orbit_url, dem_type))
	master_zip_urls.append(zip_url)
	master_orbit_urls.append(orbit_url)
    

    print("Processing Slaves")
    for slc_id in slave_slcs:
	result = query_grq(slc_id)[0]['_source']
        track = result['metadata']['track_number']
	slave_orbit_number = result['metadata']['orbitNumber']
        zip_url = get_prod_url(result['urls'], result['metadata']['archive_filename'])
        orbit_url = get_orbit_url (slc_id, track)
        dem_type = get_dem_type(result)
        print("%s : %s : %s : %s : %s : %s" %(slave_orbit_number, slc_id, track, zip_url, orbit_url, dem_type))
        slave_zip_urls.append(zip_url)
        slave_orbit_urls.append(orbit_url)


    print("\n\n\n Master Zips:")
    print_list(master_zip_urls)
    print("\nSlave Zip:")
    print_list(slave_zip_urls)
    print("\nMaster Orbit:")
    print_list(master_orbit_urls)
    print("\nSlave Orbit:")
    print_list(slave_orbit_urls)

    print("\n\n")
    # get orbit type
    orbit_type = 'poeorb'
    for o in master_orbit_urls+ slave_orbit_urls:
	print(o)
	if RESORB_RE.search(o):
	    orbit_type = 'resorb'
     	    break

    print(orbit_type)
    if orbit_type == 'resorb':
	logger.info("Precise orbit required. Filtering job configured with restituted orbit.")
    #else:
	swathnums=[1,2,3]
	ifg_hash = hashlib.md5(json.dumps([
                                    IFG_ID_TMPL,
                                    master_zip_urls[-1],
                                    master_orbit_urls[-1],
                                    slave_zip_urls[-1],
                                    slave_orbit_urls[-1],
                                    #bboxes[-1],
                                    #auto_bboxes[-1],
                                    projects[-1],
                                    dem_type
                                ])).hexdigest()
	ifg_id = IFG_ID_TMPL.format('M', len(master_slcs), len(slave_slcs), track, master_orbit_number, slave_orbit_number, swathnums, orbit_type, ifg_hash[0:4])
        
    return context['project'], True, ifg_id, master_zip_urls, master_orbit_urls, slave_zip_urls, slave_orbit_urls, context['bbox'], context['wuid'], context['job_num']


def get_prod_url (urls, archive_file):
    prod_url = urls[0]
    if len(urls) > 1:
     	for u in urls:
      	    if u.startswith('s3://'):
                prod_url = u
                break
        #print("prod_url : %s" %prod_url)
    zip_url = "%s/%s" % (prod_url, archive_file)
    return zip_url

def get_orbit_url (slc_id, track):
    orbit_url = None
    orbit_dict = {}

    try:

        match = SLC_RE.search(slc_id)
        #print("match : %s" %match)
        if not match:
            raise RuntimeError("Failed to recognize SLC ID %s." % h['_id'])
        slc_start_dt = datetime(int(match.group('start_year')),
                                int(match.group('start_month')),
                                int(match.group('start_day')),
                                int(match.group('start_hour')),
                                int(match.group('start_min')),
                                int(match.group('start_sec')))
        #print("slc_start_dt : %s" %slc_start_dt)

        slc_end_dt = datetime(int(match.group('end_year')),
                              int(match.group('end_month')),
                              int(match.group('end_day')),
                              int(match.group('end_hour')),
                              int(match.group('end_min')),
                              int(match.group('end_sec')))

        #print("slc_end_dt : %s" %slc_end_dt)
	dt_orb = "%s_%s" % (slc_start_dt.isoformat(), slc_start_dt.isoformat())

        
 	if dt_orb not in orbit_dict:
            match = SLC_RE.search(slc_id)
            if not match:
                raise RuntimeError("Failed to recognize SLC ID %s." % slc_id)
            mission = match.group('mission')
     	    print(mission)
            orbit_url = fetch("%s.0" % slc_start_dt.isoformat(),
                                           "%s.0" % slc_end_dt.isoformat(),
                                           mission=mission, dry_run=True)

            orbit_dict[dt_orb] = orbit_url

            logger.info("REF_DT_ORB : %s VALUE : %s"%(dt_orb, orbit_dict[dt_orb]))
            if orbit_dict[dt_orb] is None:
                raise RuntimeError("Failed to query for an orbit URL for track {} {} {}.".format(track,
                                   slc_start_dt, slc_end_dt))

    except Exception as e:
	print(str(e))

    return orbit_url
'''


if __name__ == '__main__':
    try: status = initiate_standard_product_job("_context.json")
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)


