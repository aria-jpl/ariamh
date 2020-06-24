#!/usr/bin/env python 
from __future__ import division
from builtins import str
from builtins import range
from builtins import object
import os, sys, time, json, requests, logging
import re, traceback, argparse, copy, bisect
from xml.etree import ElementTree
#from hysds_commons.job_utils import resolve_hysds_job
#from hysds.celery import app
from UrlUtils import UrlUtils
import hashlib
import shapely
import pickle
from shapely.geometry import shape, Polygon, MultiPolygon, mapping
from shapely.ops import cascaded_union
import datetime, time
import dateutil.parser
from datetime import datetime, timedelta
#import groundTrack
from osgeo import ogr, osr
import elasticsearch
import lightweight_water_mask
import pytz
from dateutil import parser
import collections, operator
from collections import OrderedDict
import random

'''
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())
'''

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class NoIntersectException(Exception):
    pass

class InvalidOrbitException(Exception):
    pass

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('util')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


DATE_RE = re.compile(r'(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<min>\d{2}):(?P<sec>\d{2}).*$')

SLC_RE = re.compile(r'(?P<mission>S1\w)_IW_SLC__.*?' +
                    r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'_(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_.*$')

# acquisition-S1A_IW_SLC__1SSV_20160203T004751_20160203T004818_009775_00E4B2_A0B8
ACQ_RE = re.compile(r'(?P<mission>acquisition-S1\w)_IW_SLC__.*?' +
                    r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'_(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_.*$')



#acquisition-Sentinel-1B_20161003T020729.245_35_IW-esa_scihub
ACQ_RE_v2_0= re.compile(r'(?P<mission>acquisition-Sentinel-1\w)_' +
                    r'(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2}).*$')

IFG_ID_TMPL = "S1-IFG_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s123-{}-{}-standard_product"
RSP_ID_TMPL = "S1-SLCP_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s{}-{}-{}"

BASE_PATH = os.path.dirname(__file__)
MISSION = 'S1A'



class ACQ(object):
    def __init__(self, acq_id, download_url, tracknumber, location, starttime, endtime, direction, orbitnumber, identifier, pol_mode, pv, sensingStart, sensingStop, ingestiondate, platform = None  ):
        print("ACQ : %s %s %s" %(acq_id, starttime, endtime))
        if isinstance(acq_id, tuple) or isinstance(acq_id, list):
            acq_id = acq_id[0]
        self.acq_id=acq_id,
        self.download_url = download_url
        self.tracknumber = tracknumber
        self.location= location
        self.starttime = get_time_str(starttime)
        self.endtime = get_time_str(endtime)
        self.pv = pv
        self.pol_mode = pol_mode
        self.direction = direction
        self.orbitnumber = orbitnumber
        self.identifier = identifier
        self.platform = platform
        self.sensingStart = sensingStart
        self.sensingStop = sensingStop
        self.ingestiondate = ingestiondate
        self.covers_only_water = lightweight_water_mask.covers_only_water(location)
        self.covers_only_land = lightweight_water_mask.covers_only_land(location)
        
        #print("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s" %(acq_id, download_url, tracknumber, location, starttime, endtime, direction, orbitnumber, identifier, pv))



# set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())




BASE_PATH = os.path.dirname(__file__)
MOZART_ES_ENDPOINT = "MOZART"
GRQ_ES_ENDPOINT = "GRQ"

def print_acq(acq):
    print("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" %(acq.acq_id, acq.download_url, acq.tracknumber, acq.location, acq.starttime, acq.endtime, acq.direction, acq.orbitnumber, acq.identifier, acq.pol_mode, acq.pv))

def group_acqs_by_orbit_number_from_metadata(frames):
    print("group_acqs_by_orbit_number_from_metadata")
    return group_acqs_by_orbit_number(create_acqs_from_metadata(frames))

def group_acqs_by_track_date_from_metadata(frames):
    print("group_acqs_by_track_date_from_metadata")
    return group_acqs_by_track_multi_date(create_acqs_from_metadata(frames))

def group_acqs_by_track_date(acqs):
    print("\ngroup_acqs_by_track_date")
    grouped = {}
    acqs_info = {}
    for acq in acqs:
        acqs_info[acq.acq_id] = acq
        match = SLC_RE.search(acq.identifier)
        
        if not match:
            raise RuntimeError("Failed to recognize SLC ID %s." % h['_id'])
        print("group_acqs_by_track_date : Year : %s Month : %s Day : %s" %(int(match.group('start_year')), int(match.group('start_month')), int(match.group('start_day'))))

        day_dt = datetime(int(match.group('start_year')),
                          int(match.group('start_month')),
                          int(match.group('start_day')),
                          0, 0, 0)
        print("day_dt : %s " %day_dt)
        #bisect.insort(grouped.setdefault(fields['metadata']['track_number'], {}).setdefault(day_dt, []), h['_id'])
        bisect.insort(grouped.setdefault(acq.tracknumber, {}).setdefault(day_dt, []), acq.acq_id)
    return {"grouped": grouped, "acq_info" : acqs_info}

def get_polarisation(pol_mode):
    modes = pol_mode.strip().split(' ')
    if "VV" in modes or "vv" in modes:
        return "vv"
    elif "HH" in modes or "hh" in modes:
        return "hh"
    else:
        raise RunTimeError("Invalid Polarization for the acquisition : {}".format(pol_mode)) 
       
def group_acqs_by_track_multi_date(acqs):
    print("\ngroup_acqs_by_track_multi_date")
    grouped = {}
    acqs_info = {}
    for acq in acqs:
        print("group_acqs_by_track_multi_date : acq.acq_id : %s" %acq.acq_id)
        if isinstance(acq.acq_id, tuple) or isinstance(acq.acq_id, list):
            acq.acq_id = acq.acq_id[0]
        print("group_acqs_by_track_multi_date : acq.acq_id 2: %s" %acq.acq_id)
        acqs_info[acq.acq_id] = acq
        match = SLC_RE.search(acq.identifier)

        if not match:
            raise RuntimeError("Failed to recognize SLC ID %s." % h['_id'])
        print("group_acqs_by_track_multi_date : Year : %s Month : %s Day : %s" %(int(match.group('start_year')), int(match.group('start_month')), int(match.group('start_day'))))

        day_dt = datetime(int(match.group('start_year')),
                          int(match.group('start_month')),
                          int(match.group('start_day')),
                          0, 0, 0)
        print("day_dt : %s " %day_dt)
        day_dt_yesterday = day_dt + timedelta(days=-1)
        day_dt_tomorrow = day_dt + timedelta(days=1)
        print("day_dt_yesterday : %s " %day_dt_yesterday)
        print("day_dt_tomorrow : %s" %day_dt_tomorrow)
        if acq.tracknumber in list(grouped.keys()):
            print("acq.tracknumber : %s grouped[acq.tracknumber].keys() : %s " %(acq.tracknumber, list(grouped[acq.tracknumber].keys())))
            for d in  list(grouped[acq.tracknumber].keys()):
                if d == day_dt_yesterday:
                    print("day_dt_yesterday exists. updating day_dt to day_dt_yesterday")
                    day_dt = day_dt_yesterday
                elif d == day_dt_tomorrow:
                    print("day_dt_tomorrow exists. updating day_dt to day_dt_tomorrow")
                    day_dt = day_dt_tomorrow

        #print("start_date : %s" %datetime.strptime(acq.starttime, '%Y-%m-%d'))
        #bisect.insort(grouped.setdefault(fields['metadata']['track_number'], {}).setdefault(day_dt, []), h['_id'])
        bisect.insort(grouped.setdefault(acq.tracknumber, {}).setdefault(day_dt, []), acq.acq_id)
    return {"grouped": grouped, "acq_info" : acqs_info}



def group_acqs_by_orbit_number(acqs):
    #print(acqs)
    grouped = {}
    acqs_info = {}
    for acq in acqs:
        acqs_info[acq.acq_id] = acq
        print("acq_id : %s track_number : %s orbit_number : %s acq_pv : %s" %(acq.acq_id, acq.tracknumber, acq.orbitnumber, acq.pv))
        bisect.insort(grouped.setdefault(acq.tracknumber, {}).setdefault(acq.orbitnumber, {}).setdefault(acq.pv, []), acq.acq_id)
        '''
        if track in grouped.keys():
            if orbitnumber in grouped[track].keys():
                if pv in grouped[track][orbitnumber].keys():
                    grouped[track][orbitnumber][pv] = grouped[track][orbitnumber][pv].append(slave_acq)
                else:
                    slave_acqs = [slave_acq]
                    slave_pv = {}
                
                    grouped[track][orbitnumber] = 
        '''
    return {"grouped": grouped, "acq_info" : acqs_info}



def update_acq_pv(id, ipf_version):
    try:
        uu = UrlUtils()
        es_url = uu.rest_url
        #es_index = "grq_*_{}".format(index_suffix.lower())
        es_index = "grq_v2.0_acquisition-s1-iw_slc"
        _type = "acquisition-S1-IW_SLC"
        ES = elasticsearch.Elasticsearch(es_url)


        ES.update(index=es_index, doc_type=_type, id=id,
              body={"doc": {"metadata": {"processing_version": ipf_version}}}) 
        print("Updated IPF %s of acq %s successfully" %(id, ipf_version))    
    except Exception as err:
        print("ERROR : updationg ipf : %s" %str(err))


def import_file_by_osks(url):
    osaka.main.get(url)

def check_prod_avail(session, link):
    """
    check if product is currently available or in long time archive
    :param session:
    :param link:
    :return:
    """

    product_url = "{}$value".format(link)
    response = session.get(product_url, verify=False)
    return response.status_code


def get_scihub_manifest(session, info):
    """Get manifest information."""

    # disable extraction of manifest (takes too long); will be
    # extracted when needed during standard product pipeline

    # print("info: {}".format(json.dumps(info, indent=2)))
    manifest_url = "{}Nodes('{}')/Nodes('manifest.safe')/$value".format(info['met']['alternative'],
                                                                             info['met']['filename'])
    manifest_url2 = manifest_url.replace('/apihub/', '/dhus/')
    for url in (manifest_url2, manifest_url):
        response = session.get(url, verify=False)
        print("url: %s" % response.url)
        if response.status_code == 200:
            break
    response.raise_for_status()
    return response.content


def get_scihub_namespaces(xml):
    """Take an xml string and return a dict of namespace prefixes to
       namespaces mapping."""

    nss = {}
    matches = re.findall(r'\s+xmlns:?(\w*?)\s*=\s*[\'"](.*?)[\'"]', xml)
    for match in matches:
        prefix = match[0]; ns = match[1]
        if prefix == '': prefix = '_default'
        nss[prefix] = ns
    return nss


def get_scihub_ipf(manifest):
    # append processing version (ipf)
    ns = get_scihub_namespaces(manifest)
    x = fromstring(manifest)
    ipf = x.xpath('.//xmlData/safe:processing/safe:facility/safe:software/@version', namespaces=ns)[0]

    return ipf


def get_dataset_json(met, version):
    """Generated HySDS dataset JSON from met JSON."""

    return {
        "version": version,
        "label": met['id'],
        "location": met['location'],
        "starttime": met['sensingStart'],
        "endtime": met['sensingStop'],
    }


def extract_scihub_ipf(met):
    user = None
    password = None

    # get session
    session = requests.session()
    if None not in (user, password): session.auth = (user, password)

    ds = get_dataset_json(met, version="v2.0")

    info = {
        'met': met,
        'ds': ds
    }

    prod_avail = check_prod_avail(session, info['met']['alternative'])
    if prod_avail == 200:
        manifest = get_scihub_manifest(session, info)
    elif prod_avail == 202:
        print("Got 202 from SciHub. Product moved to long term archive.")
        raise Exception("Got 202. Product moved to long term archive.")
    else:
        print("Got code {} from SciHub".format(prod_avail))
        raise Exception("Got code {}".format(prod_avail))

    ipf = get_scihub_ipf(manifest)
    return ipf

def get_area(coords):
    '''get area of enclosed coordinates- determines clockwise or counterclockwise order'''
    print("get_area : coords : %s" %coords)
    n = len(coords) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        #print("i : %s j: %s, coords[i][1] : %s coords[j][0] : %s coords[j][1] : %s coords[i][0] : %s"  %(i, j, coords[i][1], coords[j][0], coords[j][1], coords[i][0]))
        area += coords[i][1] * coords[j][0]
        area -= coords[j][1] * coords[i][0]
    #area = abs(area) / 2.0
    return area / 2

def get_env_box(env):

    #print("get_env_box env " %env)
    bbox = [
        [ env[3], env[0] ],
        [ env[3], env[1] ],
        [ env[2], env[1] ],
        [ env[2], env[0] ],
    ]
    print("get_env_box box : %s" %bbox)
    return bbox

def isTrackSelected(acqs_land, total_land):
    selected = False
    sum_of_acq_land = 0

    for acq_land in acqs_land:
        sum_of_acq_land+= acq_land

    delta = abs(sum_of_acq_land - total_land)
    if delta/total_land<.01:
        selected = True

    return selected

def get_ipf_count(acqs):
    pv_list = []
    no_pv_list = []
    for acq in acqs:
        if acq.pv:
            pv_list.append(acq.pv)
        else:
            pv = get_processing_version(acq.identifier)
            if pv:
                update_acq_pv(acq.acq_id, pv)
                pv_list.append(pv)
            else:
                err_msg = "IPF version NOT found for %s" %acq.acq_id
                no_pv_list.append(acq.acq_id)
                print(err_msg)

    if len(no_pv_list)>0:
        err_msg = "IPF NOT Found for acquisitions : %s" %no_pv_list
        raise RuntimeError(err_msg)

    return len(list(set(pv_list)))

def get_ipf_count_by_acq_id(acq_ids, acq_info):
    acqs = []
    for acq_id in acq_ids:
        acq = acq_info[acq_id]
        acqs.append(acq)
    return get_ipf_count(acqs)

def get_pol_data_from_acqs(acqs):
    pol_data = []
    for acq in acqs:
        pol = acq.pol_mode.strip().lower()
        if pol not in pol_data:
            pol_data.append(pol)

    if len(pol_data)==0 or len(pol_data)>1:
        err_msg = "Found Multiple Polarization or No Polarization : {}".format(pol_data)
        raise RuntimeError(err_msg)

    return pol_data[0]




def get_union_data_from_acqs(acqs):
    starttimes = []
    endtimes = []
    polygons = []
    track = None
    direction = None
    orbitnumber = None
    pv_list = []
    no_pv_list = []


    for acq in acqs:
        if acq.pv:
            pv_list.append(acq.pv)
        else:
            pv = get_processing_version(acq.identifier)
            if pv:
                update_acq_pv(acq.acq_id, pv)
                pv_list.append(pv)
            else:
                no_pv_list.append(acq.identifier)

        starttimes.append(get_time(acq.starttime))
        endtimes.append(get_time(acq.endtime)) 
        polygons.append(acq.location)
        track = acq.tracknumber
        direction = acq.direction
        orbitnumber =acq.orbitnumber
    
    if len(no_pv_list)>0:
        err_msg = "IPF NOT Found for : %s" %no_pv_list
        raise RuntimeError(err_msg)

    starttime = sorted(starttimes)[0]
    endtime = sorted(endtimes, reverse=True)[0]
    location = get_union_geometry(polygons)
    ipf_count = len(list(set(pv_list)))

    return ipf_count, starttime.strftime("%Y-%m-%d %H:%M:%S"), endtime.strftime("%Y-%m-%d %H:%M:%S"), location, track, direction, orbitnumber 


def create_acq_obj_from_metadata(acq):
    #create ACQ(acq_id, download_url, tracknumber, location, starttime, endtime, direction, orbitnumber, identifier, pv)
    #print("ACQ = %s\n" %acq)
    acq_data = acq #acq['fields']['partial'][0]
    missing_pcv_list = list()
    acq_id = acq['id']
    print("logger level : %s" %logger.level)
    print("Creating Acquisition Obj for acq_id : %s : %s" %(type(acq_id), acq_id))
    '''
    match = SLC_RE.search(acq_id)
    if not match:
        print("SLC_RE : %s" %SLC_RE)
        print("Error : No Match : %s" %acq_id)
        return None
    print("SLC_MATCHED")
    '''

    download_url = acq_data['metadata']['download_url']
    track = acq_data['metadata']['track_number']
    location = acq_data['metadata']['location']
    starttime = acq_data['starttime']
    endtime = acq_data['endtime']
    sensingStop = acq_data['metadata']['sensingStop']
    sensingStart = acq_data['metadata']['sensingStart']
    ingestiondate = acq_data['metadata']['ingestiondate']
    direction = acq_data['metadata']['direction']
    orbitnumber = acq_data['metadata']['orbitNumber']
    platform = acq_data['metadata']['platform']
    identifier = acq_data['metadata']['identifier']
    pol_mode = get_polarisation(acq_data['metadata']['polarisationmode'])
    print("Polarisation : {} with Modes : {}".format(pol_mode, acq_data['metadata']['polarisationmode']))

    pv = None
    if "processing_version" in  acq_data['metadata']:
        pv = acq_data['metadata']['processing_version']
        if pv:
            print("pv found in metadata : %s" %pv)
        else:
            print("pv NOT in metadata,so calling ASF")
            pv = get_processing_version(identifier, acq_data['metadata'])
            print("ASF returned pv : %s" %pv)
            if pv:
                update_acq_pv(acq_id, pv)
    else:
        missing_pcv_list.append(acq_id)
        print("pv NOT in metadata,so calling ASF")
        pv = get_processing_version(identifier, acq_data['metadata'])
        print("ASF returned pv : %s" %pv)
        if pv:
            update_acq_pv(acq_id, pv) 
    return ACQ(acq_id, download_url, track, location, starttime, endtime, direction, orbitnumber, identifier, pol_mode, pv, sensingStart, sensingStop, ingestiondate, platform)
    

    '''
    host_ip = get_container_host_ip()
    print(host_ip)

    gateway_ip = get_gateway_ip()
    print(gateway_ip)

    in_container = running_in_container()
    print(in_container)

    ip_routes_output = check_output(["ip", "route", "show", "default", "0.0.0.0/0"])
    assert type(ip_routes_output) == bytes

    ip_routes_output_str = ip_routes_output.decode('utf-8')
    assert type(ip_routes_output_str) == str
    '''


def create_acqs_from_metadata(frames):
    acqs = list()
    #print("frame length : %s" %len(frames))
    for acq in frames:
        print("create_acqs_from_metadata : %s" %acq['id'])
        acq_obj = create_acq_obj_from_metadata(acq)
        if acq_obj:
            acqs.append(acq_obj)
    return acqs

def get_result_dict(aoi=None, track=None, track_dt=None):
    result = {}
    result['aoi'] = aoi
    result['track'] = track
    result['orbit_name']=None
    #result['secondary_orbit']=None
    #result['primary_track_dt']  = None
    #result['secondary_track_dt']  = None
    result['acq_union_land_area'] = None
    result['acq_union_aoi_intersection'] = None
    result['ACQ_POEORB_AOI_Intersection'] = None   
    result['ACQ_Union_POEORB_Land'] = None
    result['Track_POEORB_Land'] = None
    result['Track_AOI_Intersection'] = None
    result['res'] = None
    result['WATER_MASK_PASSED'] = None
    result['matched'] = None
    result['BL_PASSED'] = None
    result['primary_ipf_count'] = None
    result['secondary_ipf_count'] = None
    result['candidate_pairs'] = None
    result['fail_reason'] = ''
    result['comment'] = ''
    result['dt']=track_dt
    result['result']=None
    result['union_geojson']=None
    result['delta_area']=None
    result['orbit_quality_check_passed']=None
    result['area_threshold_passed'] = None
    result['list_master_dt'] = None
    result['list_slave_dt'] = None
    result['master_count'] = 0
    result['slave_count'] = 0
    result['master_orbit_file'] = ''
    result['slave_orbit_file'] = ''
    result['master_dropped_ids'] = list()
    result['slave_dropped_ids'] = list()
    result['full_id_hash'] = ''

    '''
    result['ACQ_POEORB_AOI_Intersection_primary'] = None
    result['ACQ_Union_POEORB_Land_primary'] = None
    result['ACQ_POEORB_AOI_Intersection_secondary'] = None
    result['ACQ_Union_POEORB_Land_secondary'] = None
    result['Track_POEORB_Land_secondary'] = None
    result['Track_AOI_Intersection_secondary'] = None
    result['Track_POEORB_Land_secondary'] = None
    result['Track_AOI_Intersection_secondary'] = None    
    '''
    return result


def get_acq_ids(acqs):
    acq_ids = list()
    for acq in acqs:
        acq_id = acq.acq_id
        if isinstance(acq_id, tuple) or isinstance(acq_id, list):
            acq_id = acq_id[0]
        acq_ids.append(acq_id)
    return acq_ids


def gen_hash(master_scenes, slave_scenes):
    '''Generates a hash from the master and slave scene list'''
    master = [x.replace('acquisition-', '') for x in master_scenes]
    slave = [x.replace('acquisition-', '') for x in slave_scenes]
    master = pickle.dumps(sorted(master))
    slave = pickle.dumps(sorted(slave))
    return '{}_{}'.format(hashlib.md5(master).hexdigest(), hashlib.md5(slave).hexdigest())


def get_ifg_hash(master_slcs,  slave_slcs):

    master_ids_str=""
    slave_ids_str=""

    for slc in sorted(master_slcs):
        print("get_ifg_hash : master slc : %s" %slc)
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]

        if master_ids_str=="":
            master_ids_str= slc
        else:
            master_ids_str += " "+slc

    for slc in sorted(slave_slcs):
        print("get_ifg_hash: slave slc : %s" %slc)
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]

        if slave_ids_str=="":
            slave_ids_str= slc
        else:
            slave_ids_str += " "+slc

    id_hash = hashlib.md5(json.dumps([
            master_ids_str,
            slave_ids_str
            ]).encode("utf8")).hexdigest()
    return id_hash

def get_ifg_hash_from_acqs(master_acqs,  slave_acqs):

    master_slcs = []
    slave_slcs = []

    if len(master_acqs)>0:
        master_slcs = get_slc_list_from_acq_list(master_acqs)
    if len(slave_acqs)>0:
        slave_slcs = get_slc_list_from_acq_list(slave_acqs)

    return get_ifg_hash(master_slcs,  slave_slcs)


def dataset_exists(id, index_suffix):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    uu = UrlUtils()
    es_url = uu.rest_url
    es_index = "grq_*_{}".format(index_suffix.lower())
    
    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } },
                ]
            }
        },
        "fields": [],
    }

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code == 200:
        result = r.json()
        total = result['hits']['total']
    else:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        if r.status_code == 404: total = 0
        else: r.raise_for_status()
    return False if total == 0 else True

def get_dataset(id, index_suffix):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    uu = UrlUtils()
    es_url = uu.rest_url
    es_index = "grq_*_{}".format(index_suffix.lower())
    #es_index = "grq"

    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } }
                ]
            }
        },
        "fields": []
    }

    print(query)

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
    return result

def get_dataset_by_hash(ifg_hash, dataset_type, es_index="grq"):
    """Query for existence of dataset by ID."""

    uu = UrlUtils()
    es_url = uu.rest_url
    #es_index = "{}_{}_s1-ifg".format(uu.grq_index_prefix, version)

    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{"metadata.full_id_hash.raw": ifg_hash} },
                    { "term":{"dataset.raw": dataset_type} }
                ]
            }
        }
        
    }

    logger.info(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    logger.info("search_url : %s" %search_url)

    r = requests.post(search_url, data=json.dumps(query))
    r.raise_for_status()

    if r.status_code != 200:
        logger.info("Failed to query %s:\n%s" % (es_url, r.text))
        logger.info("query: %s" % json.dumps(query, indent=2))
        logger.info("returned: %s" % r.text)
        raise RuntimeError("Failed to query %s:\n%s" % (es_url, r.text))
    result = r.json()
    logger.info(result['hits']['total'])
    return result



def get_complete_track_aoi_by_hash(new_ifg_hash, track, aoi):
    logger.info("get_complete_track_aoi_by_hash : new_ifg_hash {} track {} aoi {}".format(new_ifg_hash, track, aoi))
    es_index="grq_*_s1-gunw-acqlist-audit_trail"
    result = get_dataset_by_hash(new_ifg_hash, "S1-GUNW-acqlist-audit_trail", es_index)
    total = result['hits']['total']
    logger.info("check_slc_status_by_hash : total : %s" %total)
    if total>0:
        for i in range(total):
            found_id  = result['hits']['hits'][i]["_id"]
            logger.info("get_complete_track_aoi_by_hash: audit trail found: %s" %found_id)
            metadata = result['hits']['hits'][i]["_source"]["metadata"]
            this_track = metadata["track_number"]
            this_aoi = metadata["aoi"]
            logger.info("With Track and AOI : %s, %s" %(this_track, this_aoi))
            if isinstance(this_track, list):
                track.extend(this_track)
            else:
                track.append(this_track)
            if isinstance(this_aoi, list):
                aoi.extend(this_aoi)
            else:
                aoi.append(this_aoi)

    return list(set(track)), list(set(aoi))


def get_dataset(id):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    uu = UrlUtils()
    es_url = uu.rest_url
    #es_index = "grq_*_{}".format(index_suffix.lower())
    es_index = "grq"

    # query
    query = {
        "query":{

            "bool":{
                "must":[
                    { "term":{ "_id": id } }
                ]
            }
        },
        "fields": []
    }

    print(query)

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
    return result


def query_es(query, es_index=None):
    """Query ES."""
    uu = UrlUtils()
    es_url = uu.rest_url
    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
    url = "{}/_search?search_type=scan&scroll=60&size=100".format(rest_url)
    if es_index:
        url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    #print("url: {}".format(url))
    r = requests.post(url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    #r.raise_for_status()
    scan_result = r.json()
    #print("scan_result: {}".format(json.dumps(scan_result, indent=2)))
    count = scan_result['hits']['total']
    if '_scroll_id' not in scan_result:
        print("_scroll_id not found in scan_result. Returning empty array for the query :\n%s" %query)
        return []

    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        if r.status_code != 200:
            print("Failed to query %s:\n%s" % (es_url, r.text))
            print("query: %s" % json.dumps(query, indent=2))
            print("returned: %s" % r.text)
            r.raise_for_status()

        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits


def query_es2(query, es_index=None):
    """Query ES."""
    print(query)
    uu = UrlUtils()
    es_url = uu.rest_url
    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
    url = "{}/_search?search_type=scan&scroll=60&size=100".format(rest_url)
    if es_index:
        url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    #print("url: {}".format(url))
    r = requests.post(url, data=json.dumps(query))
    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    #r.raise_for_status()
    scan_result = r.json()
    #print("scan_result: {}".format(json.dumps(scan_result, indent=2)))
    count = scan_result['hits']['total']
    if count == 0:
        return []

    if '_scroll_id' not in scan_result:
        print("_scroll_id not found in scan_result. Returning empty array for the query :\n%s" %query)
        return []

    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        if r.status_code != 200:
            print("Failed to query %s:\n%s" % (es_url, r.text))
            print("query: %s" % json.dumps(query, indent=2))
            print("returned: %s" % r.text)
            r.raise_for_status()

        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits

def print_groups(grouped_matched):
    for track in grouped_matched["grouped"]:
        print("\nTrack : %s" %track)
        for day_dt in sorted(grouped_matched["grouped"][track], reverse=True):
            print("\tDate : %s" %day_dt)
            for acq in grouped_matched["grouped"][track][day_dt][pv]:
                print("\t\t%s" %(acq[0]))


def get_complete_grq_data(id):
    uu = UrlUtils()
    es_url = uu.rest_url
    es_index = "grq"
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "_id": id
              }
            }
          ]
        }
      }
    }


    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        err_str = "Failed to query %s:\n%s" % (es_url, r.text)
        err_str += "\nreturned: %s" % r.text
        print(err_str)
        print("query: %s" % json.dumps(query, indent=2))
        #r.raise_for_status()
        raise RuntimeError(err_str)
    '''
    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()
    '''

    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits']

def get_partial_grq_data(id):
    uu = UrlUtils()
    es_url = uu.rest_url
    es_index = "grq"

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

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        err_str = "Failed to query %s:\n%s" % (es_url, r.text)
        err_str += "\nreturned: %s" % r.text
        print(err_str)
        print("query: %s" % json.dumps(query, indent=2))
        #r.raise_for_status()
        raise RuntimeError(err_str)

    '''
    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()
    '''

    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits'][0]

def get_acquisition_data(id):
    uu = UrlUtils()
    es_url = uu.rest_url
    es_index = "grq_*_*acquisition*"
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "_id": id
              }
            }
          ]
        }
      },
      "partial_fields": {
        "partial": {
          "include": [
            "id",
            "dataset_type",
            "dataset",
            "metadata",
            "city",
            "continent"
          ]
        }
      }
    }


    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))


    if r.status_code != 200:
        err_str = "Failed to query %s:\n%s" % (es_url, r.text)
        err_str += "\nreturned: %s" % r.text
        print(err_str)
        print("query: %s" % json.dumps(query, indent=2))
        #r.raise_for_status()
        raise RuntimeError(err_str)
    '''
    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()
    '''
    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits']


def group_acqs_by_track(frames):
    grouped = {}
    acq_info = {}
    #print("frame length : %s" %len(frames))
    for acq in frames:
        #print("ACQ : %s" %acq)
        acq_data = acq # acq['fields']['partial'][0]
        acq_id = acq['id']
        #print("acq_id : %s : %s" %(type(acq_id), acq_id))
        '''
        match = SLC_RE.search(acq_id)
        if not match:
            print("No Match : %s" %acq_id)
            continue
        '''
        download_url = acq_data['metadata']['download_url']
        track = acq_data['metadata']['track_number']
        location = acq_data['metadata']['location']
        starttime = acq_data['starttime']
        endtime = acq_data['endtime']
        direction = acq_data['metadata']['direction']
        orbitnumber = acq_data['metadata']['orbitNumber']
        identifier = acq['metadata']['identifier']
        platform = acq_data['metadata']['platform']
        sensingStop = acq_data['metadata']['sensingStop']
        sensingStart = acq_data['metadata']['sensingStart']
        ingestiondate = acq_data['metadata']['ingestiondate']
        pol_mode = get_polarisation(acq_data['metadata']['polarisationmode'])
        print("Polarisation : {} with Modes : {}".format(pol_mode, acq_data['metadata']['polarisationmode']))

        pv = None
        if "processing_version" in  acq_data['metadata']:
            pv = acq_data['metadata']['processing_version']
        this_acq = ACQ(acq_id, download_url, track, location, starttime, endtime, direction, orbitnumber, identifier, pv, pol_mode, sensingStart, sensingStop, ingestiondate, platform)
        acq_info[acq_id] = this_acq

        #print("Adding %s : %s : %s : %s" %(track, orbitnumber, pv, acq_id))
        #print(grouped)
        bisect.insort(grouped.setdefault(track, []), acq_id)
        '''
        if track in grouped.keys():
            if orbitnumber in grouped[track].keys():
                if pv in grouped[track][orbitnumber].keys():
                    grouped[track][orbitnumber][pv] = grouped[track][orbitnumber][pv].append(slave_acq)
                else:
                    slave_acqs = [slave_acq]
                    slave_pv = {}
                
                    grouped[track][orbitnumber] = 
        '''
    return {"grouped": grouped, "acq_info" : acq_info}

def filter_acq_ids(acq_info, acq_ids, ssth=3):
    print("\nfilter_acq_ids: acq_ids: %s" %(acq_ids))
    dedup_acqs = dict()
    last_id = None
    last_sensing_start = None
    temp_dict = {}
    dropped_ids = []
    filtered_ids = []

    
    ''' First we need to have all the acqusitions sorted by sensing_start time '''
    for acq_id in acq_ids:
        print("type(acq_id) : %s" %type(acq_id))
        if type(acq_id) == 'tuple' or type(acq_id)=='list' or isinstance(acq_id, tuple)  or isinstance(acq_id, list):
            acq_id = acq_id[0]
        acq = acq_info[acq_id]
        sensing_start = get_time(acq.sensingStart[:-1]) if acq.sensingStart.endswith('Z') else get_time(acq.sensingStart)

        #get_time(acq.sensingStart)

        '''
        datetime.strptime(
            acq.sensingStart[:-1] if acq.sensingStart.endswith('Z') else acq.sensingStart,  "%Y-%m-%dT%H:%M:%S.%f"
        )
        '''
        temp_dict[acq_id] = sensing_start
        

    sorted_x = sorted(list(temp_dict.items()), key=operator.itemgetter(0))
    sorted_temp_dict = OrderedDict(sorted_x)
    print ("sorted_temp_dict : %s " %sorted_temp_dict)

    ''' All the acquisition ids are sorted by sensing_start time in sorted_temp_dict now '''
    for acq_id, sensing_start in list(sorted_temp_dict.items()):
        print("acq_id: %s  sensing_start : %s" %(acq_id, sensing_start))
        if type(acq_id) == 'tuple' or type(acq_id)=='list':
            acq_id = acq_id[0]
        acq = acq_info[acq_id]
        
        #sensing_stop = get_time(acq.sensingStop)
        sensing_stop = get_time(acq.sensingStop[:-1]) if acq.sensingStop.endswith('Z') else get_time(acq.sensingStop)
        '''
        datetime.strptime(
            acq.sensingStop[:-1] if acq.sensingStop.endswith('Z') else acq.sensingStop,  "%Y-%m-%dT%H:%M:%S.%f"
        )       
        '''
        pv = acq.pv

        if last_id is not None:
            sec_diff = abs((sensing_start-last_sensing_start).total_seconds())
            print("sec_diff: %s" % sec_diff)
            if sec_diff < ssth:
                print("DUPLICATE SLCS : %s and %s" %(acq_id, last_id))
                if not dedup_acqs[last_id]['pv'] and not pv:
                    if dedup_acqs[last_id]['sensingStop'] > sensing_stop:
                        print("DROPPING : %s" %acq_id)
                        dropped_ids.append(acq_id)
                        continue
                    elif dedup_acqs[last_id]['sensingStop'] < sensing_stop: 
                        print("DROPPING : %s" %last_id)
                        del dedup_acqs[last_id]
                        dropped_ids.append(last_id)
                    elif dedup_acqs[last_id]['sensingStop'] == sensing_stop:
                        print("DROPPING : %s" %acq_id)
                        dropped_ids.append(acq_id)
                        continue
                elif pv and not dedup_acqs[last_id]['pv']:
                    print("DROPPING : %s" %last_id)
                    del dedup_acqs[last_id]
                    dropped_ids.append(last_id)
                elif dedup_acqs[last_id]['pv'] and not pv:
                    print("DROPPING : %s" %acq_id)
                    dropped_ids.append(acq_id)
                    continue
                elif pv and dedup_acqs[last_id]['pv']:
                    if pv > dedup_acqs[last_id]['pv']:
                        print("DROPPING : %s" %last_id)
                        del dedup_acqs[last_id]
                        dropped_ids.append(last_id)
                    elif pv < dedup_acqs[last_id]['pv']:
                        print("DROPPING : %s" %acq_id)
                        dropped_ids.append(acq_id)
                        continue
                    elif pv == dedup_acqs[last_id]['pv']:
                        if dedup_acqs[last_id]['sensingStop'] > sensing_stop:
                            print("DROPPING : %s" %acq_id)
                            dropped_ids.append(acq_id)
                            continue
                        elif dedup_acqs[last_id]['sensingStop'] < sensing_stop:
                            print("DROPPING : %s" %last_id)
                            del dedup_acqs[last_id]
                            dropped_ids.append(last_id)
                        elif dedup_acqs[last_id]['sensingStop'] == sensing_stop:
                            print("DROPPING : %s" %acq_id)
                            dropped_ids.append(acq_id)
                            continue
        last_id = acq_id
        last_sensing_start = sensing_start
        dedup_acqs[acq_id] = {
            'pv': pv,
            'sensingStop' : sensing_stop
                }
    filtered_ids = []
    for k in list(dedup_acqs.keys()):
        filtered_ids.append(k)
    print("returning : filtered_ids : %s, dropped_ids : %s" %(filtered_ids, dropped_ids))   
    return filtered_ids, dropped_ids



def get_slc_list_from_acq_list(acq_list):
    logger.info("get_acq_data_from_list")
    acq_info = {}
    slcs = []

    # Find out status of all Master ACQs, create a ACQ object with that and update acq_info dictionary 
    for acq in acq_list:
        acq_data = get_partial_grq_data(acq)['fields']['partial'][0]
        slcs.append(acq_data['metadata']['identifier'])
    return slcs


def getUpdatedTime(s, m):
    #date = dateutil.parser.parse(s, ignoretz=True)
    new_date = s + timedelta(minutes = m)
    return new_date

def getUpdatedTimeStr(s, m):
    date = dateutil.parser.parse(s, ignoretz=True)
    new_date = date + timedelta(minutes = m)
    return new_date

def is_overlap(geojson1, geojson2):
    '''returns True if there is any overlap between the two geojsons. The geojsons
    are just a list of coordinate tuples'''
    p3=0
    p1=Polygon(geojson1[0])
    p2=Polygon(geojson2[0])
    if p1.intersects(p2):
        p3 = p1.intersection(p2).area/p1.area
    return p1.intersects(p2), p3

def is_overlap_multi(geojson1, geojson2):

    print("is_overlap_multi : geojson1 : %s, \ngeojson2 : %s" %(geojson1, geojson2))
    p3=0
    intersects = False
    if type(geojson1) is tuple:
        geojson1 = geojson1[0]
    if type(geojson2) is tuple:
        geojson2 = geojson2[0]

    print("geojson1['type'] : %s geojson1['coordinates'] : %s" %(geojson1['type'],geojson1["coordinates"]))
    print("geojson2['type'] : %s geojson1['coordinates'] : %s" %(geojson2['type'],geojson2["coordinates"]))
    if geojson1['type'].upper() == "MULTIPOLYGON" and geojson2['type'].upper() == "MULTIPOLYGON":
        for cord1 in geojson1["coordinates"]:
            for cord2 in geojson2["coordinates"]:
                p3 = p3+get_intersection_area(cord1[0], cord2[0])
    elif geojson1['type'].upper() == "MULTIPOLYGON" and geojson2['type'].upper() == "POLYGON":
        for cord1 in geojson1["coordinates"]:
            p3 = p3+get_intersection_area(cord1[0], geojson2["coordinates"][0])
    elif geojson1['type'].upper() == "POLYGON" and geojson2['type'].upper() == "MULTIPOLYGON":
        for cord2 in geojson2["coordinates"]:
            p3 = p3+get_intersection_area(geojson1["coordinates"][0], cord2[0])
    elif geojson1['type'].upper() == "POLYGON" and geojson2['type'].upper() == "POLYGON":
        p3 = get_intersection_area(geojson1["coordinates"][0], geojson2["coordinates"][0])
    else:
        raise ValueError("Unknown Polygon Type : %s and %s" %(geojson1['type'], geojson2['type']))

    if p3>0:
        intersects = True
    if p3>1.0:
        print("ERROR : Intersection value between %s and %s is too high : %s" %(geojson1["coordinates"], geojson2["coordinates"], p3))
    return intersects, p3

def find_overlap_within_aoi(loc1, loc2, aoi_loc):
    '''returns True if there is any overlap between the two geojsons. The geojsons
    are just a list of coordinate tuples'''
    print("find_overlap_within_aoi : %s\n%s\n%s" %(loc1, loc2, aoi_loc))
    geojson1, env1 = get_intersection(loc1, aoi_loc)
    geojson2, env2 = get_intersection(loc2, aoi_loc)

     
    p3=0
    intersects = False
    if type(geojson1) is tuple:
        geojson1 = geojson1[0]
    if type(geojson2) is tuple:
        geojson2 = geojson2[0]

    print("geojson1['type'] : %s geojson1['coordinates'] : %s" %(geojson1['type'],geojson1["coordinates"]))
    print("geojson2['type'] : %s geojson1['coordinates'] : %s" %(geojson2['type'],geojson2["coordinates"]))
    if geojson1['type'] == "MultiPolygon" and geojson2['type'] == "MultiPolygon":
        for cord1 in geojson1["coordinates"]:
            for cord2 in geojson2["coordinates"]:
                p3 = p3+get_intersection_area(cord1[0], cord2[0])
    elif geojson1['type'] == "MultiPolygon" and geojson2['type'] == "Polygon":
        for cord1 in geojson1["coordinates"]:
            p3 = p3+get_intersection_area(cord1[0], geojson2["coordinates"][0])
    elif geojson1['type'] == "Polygon" and geojson2['type'] == "MultiPolygon":
        for cord2 in geojson2["coordinates"]:
            p3 = p3+get_intersection_area(geojson1["coordinates"][0], cord2[0])
    elif geojson1['type'] == "Polygon" and geojson2['type'] == "Polygon":
        p3 = get_intersection_area(geojson1["coordinates"][0], geojson2["coordinates"][0])
    else:
        raise ValueError("Unknown Polygon Type : %s and %s" %(geojson1['type'], geojson2['type']))
    
    if p3>0:
        intersects = True
    if p3>1.0:
        print("ERROR : Intersection value between %s and %s is too high : %s" %(geojson1["coordinates"], geojson2["coordinates"], p3)) 
    return intersects, p3


def get_intersection_area(cord1, cord2):

    print("\nget_intersection_area between %s and %s" %(cord1, cord2))
    p3 =0

    
    p1=Polygon(cord1)
    p2=Polygon(cord2)
    p1_land_area = lightweight_water_mask.get_land_area(p1)
    p2_land_area = lightweight_water_mask.get_land_area(p2)
    if not p1_land_area> 0 or not p2_land_area >0:
        return 0
    if p1.intersects(p2):
        intersection = p1.intersection(p2)
        print("intersection : %s" %intersection)
        intersection_land_area = lightweight_water_mask.get_land_area(intersection)
        p1_land_area = lightweight_water_mask.get_land_area(p1)
        print("intersection_land_area : %s p1_land_area : %s" %(intersection_land_area,p1_land_area))
        p3 = p1.intersection(p2).area/p1.area
        print("\n%s intersects %s with area : %s\n" %(p1, p2, p3))
        p3 = intersection_land_area/p1_land_area
        print("updated_land_area : %s" %p3)
    return p3

def is_within(geojson1, geojson2):
    '''returns True if there is any overlap between the two geojsons. The geojsons
    are just a list of coordinate tuples'''
    p1=Polygon(geojson1[0])
    p2=Polygon(geojson2[0])
    return p1.within(p2)


def find_overlap_match(master_acq, slave_acqs):
    #print("\n\nmaster info : %s : %s : %s :%s" %(master_acq.tracknumber, master_acq.orbitnumber, master_acq.pv, master_acq.acq_id))
    #print("slave info : ")

    total_cover_pct=0
    print("\n\n\nfind_overlap_match:")
    master_loc = master_acq.location["coordinates"]

    print("\n\nmaster id : %s master loc : %s" %(master_acq.acq_id, master_acq.location))
    overlapped_matches = {}
    for slave in slave_acqs:
        overlap = 0
        is_over = False
        print("SLAVE : %s" %slave.location)
        slave_loc = slave.location["coordinates"]
        print("\n\nslave_loc : %s" %slave_loc)
        is_over, overlap = is_overlap_multi(master_acq.location, slave.location)
        print("is_overlap : %s" %is_over)
        print("overlap area : %s" %overlap)
        if is_over:
            overlapped_matches[slave.acq_id] = slave
            total_cover_pct = total_cover_pct + overlap
            print("Overlapped slave : %s" %slave.acq_id)

    return overlapped_matches, total_cover_pct

def ref_truncated(ref_scene, matched_footprints, covth=.99):
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
    ref_geom = ogr.CreateGeometryFromJson(json.dumps(ref_scene.location))
    ref_geom_tr = ogr.CreateGeometryFromJson(json.dumps(ref_scene.location))
    ref_geom_tr.Transform(transform)
    ref_geom_tr_area = ref_geom_tr.GetArea() # in square meters
    print("Reference GeoJSON: %s" % ref_geom.ExportToJson())

    # get union geometry of all matched scenes
    matched_geoms = list()
    matched_union = None
    matched_geoms_tr = list()
    matched_union_tr = None
    ids = sorted(matched_footprints.keys())
    #ids.sort()
    #print("ids: %s" % len(ids))
    for id in ids:
        geom = ogr.CreateGeometryFromJson(json.dumps(matched_footprints[id]))
        geom_tr = ogr.CreateGeometryFromJson(json.dumps(matched_footprints[id]))
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
    print("Matched union GeoJSON: %s" % json.dumps(matched_union_geojson))
    
    # check matched_union disjointness
    if len(matched_union_geojson['coordinates']) > 1:
        print("Matched union is a disjoint geometry.")
        return True
            
    # check that intersection of reference and stitched scenes passes coverage threshold
    ref_int = ref_geom.Intersection(matched_union)
    ref_int_tr = ref_geom_tr.Intersection(matched_union_tr)
    ref_int_tr_area = ref_int_tr.GetArea() # in square meters
    print("Reference intersection GeoJSON: %s" % ref_int.ExportToJson())
    print("area (m^2) for intersection: %s" % ref_int_tr_area)
    cov = ref_int_tr_area/ref_geom_tr_area
    print("coverage: %s" % cov)
    if cov < covth:
        print("Matched union doesn't cover at least %s%% of the reference footprint." % (covth*100.))
        return True
   
    return False
def get_union_geojson_acqs(acqs):
    geoms = list()
    union = None
    for acq in acqs:
        geom = ogr.CreateGeometryFromJson(json.dumps(acq.location))
        geoms.append(geom)
        union = geom if union is None else union.Union(geom)
    union_geojson =  json.loads(union.ExportToJson())
    return union_geojson
    
def get_union_geometry(geojsons):
    """Return polygon of union of acquisition footprints."""

    # geometries are in lat/lon projection
    #src_srs = osr.SpatialReference()
    #src_srs.SetWellKnownGeogCS("WGS84")
    #src_srs.ImportFromEPSG(4326)

    # get union geometry of all scenes
    geoms = list()
    union = None
    for geojson in geojsons:
        geom = ogr.CreateGeometryFromJson(json.dumps(geojson))
        geoms.append(geom)
        union = geom if union is None else union.Union(geom)
    union_geojson =  json.loads(union.ExportToJson())
    return union_geojson

def get_acq_orbit_polygon(starttime, endtime, orbit_dir):
    pass
    
def get_intersection(js1, js2):
    print("GET_INTERSECTION")
    intersection = None
    poly_type = None
    try:
        print("intersection between :\n %s\nAND\n%s\n" %(js1, js2))
        poly1 = ogr.CreateGeometryFromJson(json.dumps(js1, indent=2, sort_keys=True))
        print("\nget_intersection : poly1 : %s" %poly1)

        poly2 = ogr.CreateGeometryFromJson(json.dumps(js2, indent=2, sort_keys=True))
        print("\nget_intersection : poly2 : %s" %poly2)
    
        intersection = poly1.Intersection(poly2)
        print("\nget_intersection : intersection : %s" %intersection)
    except Exception as err:
        err_value = "Error intersecting two polygon : %s" %str(err)
        print(err_value)
        raise RuntimeError(err_value)
    if isinstance(intersection, shapely.geometry.polygon.Polygon):
        poly_type = "POLYGON"
    if isinstance(intersection, shapely.geometry.multipolygon.MultiPolygon):
        poly_type = "MULTIPOLYGON"
    print("\nSHAPELY poly_type : %s" %poly_type)
   

    if intersection.IsEmpty():
        print("intersection is empty : %s" %intersection)
        err_msg = "util.get_intersection: intersection between %s and %s is %s" %(js1, js2, intersection)
        raise NoIntersectException(err_msg)

    if intersection is None or intersection == 'GEOMETRYCOLLECTION EMPTY':
        err_msg = "util.get_intersection: intersection between %s and %s is %s" %(js1, js2, intersection)
        raise NoIntersectException(err_msg)
   
    return json.loads(intersection.ExportToJson()), intersection.GetEnvelope()

def get_combined_polygon():
    pass

def get_time(t):

    print("get_time(t) : %s" %t)
    t = parser.parse(t).strftime('%Y-%m-%dT%H:%M:%S')
    t1 = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
    print("returning : %s" %t1)
    return t1

def get_time_str(t):

    print("get_time(t) : %s" %t)
    t = parser.parse(t).strftime('%Y-%m-%dT%H:%M:%S')
    return t


def get_time_str_with_format(t, str_format):

    print("get_time(t) : %s" %t)
    t = parser.parse(t).strftime(str_format)
    return t

def change_date_str_format(date_str, pre_format, post_format):
    try:
        t = datetime.strptime(date_str, pre_format)
        return parser.parse(t).strftime(t, post_format)
    except:
        return date_str

def get_processing_version(slc, met = None):
    pv = None
    print("get_processing_version : %s" %slc)    
    try:
        if met:
            try:
                pv = get_processing_version_from_scihub(slc, met)
                print("pv from scihub : %s" %pv)
            except Exception as err:
                print("Failed to get pv from scihub for %s : %s" %(slc, str(err)))
        if not pv:
            pv = get_processing_version_from_asf(slc)
            print("pv from asf : %s" %pv)
    except Exception as err:
        print("Error getting IPF : %s" %str(err))
    return pv

def get_processing_version_from_scihub(slc, met = None):

    ipf_string = None
    '''
    if met:
        ipf_string = extract_scihub_ipf(met)
    '''
    return ipf_string


def get_processing_version_from_asf(slc):

    ipf = None

    try:
        # query the asf search api to find the download url for the .iso.xml file
        #request_string = "https://api.daac.asf.alaska.edu/services/search/param?platform=SA,SB&processingLevel=METADATA_SLC&granule_list=S1B_IW_SLC__1SDV_20190221T151817_20190221T151844_015046_01C1D6_240D&output=json"
        request_string = 'https://api.daac.asf.alaska.edu/services/search/param?platform=SA,SB&processingLevel=METADATA_SLC&granule_list=%s&output=json' %slc
        print("get_processing_version_from_asf : request_string : %s" %request_string)
        response = requests.get(request_string)

        response.raise_for_status()
        results = json.loads(response.text)

        # download the .iso.xml file, assumes earthdata login credentials are in your .netrc file
        response = requests.get(results[0][0]['downloadUrl'])
        response.raise_for_status()

        # parse the xml file to extract the ipf version string
        root = ElementTree.fromstring(response.text.encode('utf-8'))
        ns = {'gmd': 'http://www.isotc211.org/2005/gmd', 'gmi': 'http://www.isotc211.org/2005/gmi', 'gco': 'http://www.isotc211.org/2005/gco'}
        ipf_string = root.find('gmd:composedOf/gmd:DS_DataSet/gmd:has/gmi:MI_Metadata/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:processStep/gmd:LI_ProcessStep/gmd:description/gco:CharacterString', ns).text

        if ipf_string:
            print("get_processing_version_from_asf : ipf_string : %s" %ipf_string)
            ipf = ipf_string.split('version')[1].split(')')[0].strip()
            print("get_processing_version_from_asf : ipf_string : %s" %ipf)
        else:
            print("get_processing_version_from_asf : ipf_string is NONE")
    except Exception as err:
        print("get_processing_version_from_asf ERROR: %s" %str(err))
        raise
 
        
    return ipf

def print_acquisitions(aoi_id, acqs):
    print("PRINT_ACQS")
    for acq in acqs:
        #aoi_id = acq_data['aoi_id']
        print("aoi : %s track: %s orbitnumber : %s pv: %s acq_id : %s" %(aoi_id, acq.tracknumber, acq.orbitnumber, acq.pv, acq.acq_id))
    print("\n")

def update_doc(body=None, index=None, doc_type=None, doc_id=None):
    uu = UrlUtils()
    es_url = uu.rest_url
    ES = elasticsearch.Elasticsearch(es_url)
    ES.update(index= index, doc_type= doc_type, id=doc_id,
              body=body)


def get_track(info):
    """Get track number."""

    tracks = {}
    for id in info:
        print(id)
        h = info[id]
        fields = h["_source"]
        track = fields['metadata']['track_number']
        print(track)
        tracks.setdefault(track, []).append(id)
    if len(tracks) != 1:
        print(tracks)
        
        raise RuntimeError("Failed to find SLCs for only 1 track : %s" %tracks)
    return track

def get_start_end_time(info):
    starttimes = list()
    endtimes = list()
    for id in info:
        h = info[id]
        fields = h["_source"]
        starttimes.append(fields['starttime'])
        endtimes.append(fields['endtime'])
    return sorted(starttimes)[0], sorted(endtimes)[-1]

def get_start_end_time2(acq_info, acq_ids):
    starttimes = list()
    endtimes = list()
    for acq_id in acq_ids:
        acq = acq_info[acq_id]
        starttimes.append(get_time(acq.starttime))
        endtimes.append(get_time(acq.endtime))
    return sorted(starttimes)[0], sorted(endtimes)[-1]

def get_bool_param(ctx, param):
    """Return bool param from context."""

    if param in ctx and isinstance(ctx[param], bool): return ctx[param]
    return True if ctx.get(param, 'true').strip().lower() == 'true' else False

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
    print("query: {}".format(json.dumps(query, indent=2)))
    r = requests.post(url, data=json.dumps(query))
    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    #r.raise_for_status()
    scan_result = r.json()
    
    count = scan_result['hits']['total']
    if count == 0:
        return list()

    if '_scroll_id' not in scan_result:
        print("_scroll_id not found in scan_result. Returning empty array for the query :\n%s" %query)
        return []

    scroll_id = scan_result['_scroll_id']
    hits = list()
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    if len(hits) == 0:
        time.sleep(random.randint(5,21))
        return get_metadata_try_again(query, rest_url, url)
        #raise RuntimeError("Failed to find {}.".format(id))
    return hits[0]

def get_metadata_try_again(query, rest_url, url):
    """Get SLC metadata."""

    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()

    count = scan_result['hits']['total']
    if count == 0:
        return []

    if '_scroll_id' not in scan_result:
        print("_scroll_id not found in scan_result. Returning empty array for the query :\n%s" %query)
        return []

    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        if r.status_code != 200:
            print("Failed to query %s:\n%s" % (es_url, r.text))
            print("query: %s" % json.dumps(query, indent=2))
            print("returned: %s" % r.text)
            r.raise_for_status()
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    if len(hits) == 0:
        raise RuntimeError("Failed to find {}.".format(id))
    return hits[0]

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
                for city in fields['city']:
                    if city['country_name'] is not None:
                        if city['country_name'].lower() == "united states":
                            dem_type="Ned1"
                            print("Found city in US : %s. So dem type is ned" %fields['city'][0]['country_name'].lower())
                            break
                    else:
                        print("fields['city'][0]['country_name'] is None")
        except:
            dem_type = "SRTM+v3"
        if dem_type.upper().startswith("NED"):
            break

    return dem_type
'''
def get_overlapping_slaves_query(master):
    return get_overlapping_slaves_query(master.starttime, master.endtime, master.location, master.tracknumber, master.direction, master.orbitnumber)
'''
    
def get_overlapping_slaves_query(starttime, location, track, direction, platform, master_orbitnumber, acquisition_version):
    query = {
      "partial_fields": {
        "partial": {
          "exclude": "city"
        }
      },
      "query": {
        "filtered": {
          "filter": {
            "geo_shape": {
              "location": {
                "shape": location
              }
            }
          },
          "query": {
            "bool": {
              "must": [
                {
                  "term": {
                    "dataset_type.raw": "acquisition"
                  }
                },
                {
                 "term": {
                    "metadata.platformname.raw": "Sentinel-1"
                  }
                },
                {
                  "term": {
                    "track_number": track
                  }
                },
                {
                  "term": {
                    "version.raw": acquisition_version
                  }
                },
                {
                  "term": {
                    "direction": direction
                  }
                },
                {
                  "range": {
                    "endtime": {
                      "lt": starttime
                    }
                  }
                }  
              ],
              "must_not": [
                {
                "term": {
                  "orbitNumber": master_orbitnumber
                   } 
                },
                {
                "term": {
                    "metadata.tags": "deprecated"
                    }
                }
              ]
            }
          }
        }
      }
    }



    return query

def get_overlapping_masters_query(master, slave):
    query = {
            "query": {
                "filtered": {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "dataset.raw": "acquisition-S1-IW_SLC"
                                    }
				
                                }
                            ]
                        }
                    },
                    "filter": {
 			"bool": {
			    "must": [
				{
                                "geo_shape": {
                                    "location": {
                                      "shape": master.location
                                    }
                                }},
				{ "term": { "direction": master.direction }},
	                        { "term": { "orbitNumber": master.orbitnumber }},
			        { "term": { "track_number": master.tracknumber }}
			    ]
			}
                    }
                }
            },
            "partial_fields" : {
                "partial" : {
                        "exclude": "city"
                }
            }
        }    

    return query

def get_overlapping_slaves_query_orbit(master, orbitnumber):
    query = {
            "query": {
                "filtered": {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "dataset.raw": "acquisition-S1-IW_SLC"
                                    }
				
                                }
                            ]
                        }
                    },
                    "filter": {
 			"bool": {
			    "must": [
				{
                                "geo_shape": {
                                    "location": {
                                      "shape": master.location
                                    }
                                }},
				{	
                                "range" : {
                                    "endtime" : {
                                        "lte" : master.starttime
                
                                    }
                                }},
				{ "term": { "track_number": master.tracknumber }},
                                { "term": { "orbitNumber": orbitnumber }},
				{ "term": { "direction": master.direction }}
			    ],
                        "must_not": [
                                {"term": {"orbitNumber": master_orbitnumber}},
                                {"term": {"metadata.tags": "deprecated"}}
                           ]
			}
                    }
                }
            },
            "partial_fields" : {
                "partial" : {
                        "exclude": "city"
                }
            }
        }    

    return query

def create_dataset_json(id, version, met_file, ds_file):
    """Write dataset json."""


    # get metadata
    with open(met_file) as f:
        md = json.load(f)

    ds = {
        'creation_timestamp': "%sZ" % datetime.utcnow().isoformat(),
        'version': version,
        'label': id
    }

    coordinates = None

    try:
        '''
        if 'bbox' in md:
            print("create_dataset_json : met['bbox']: %s" %md['bbox'])
            coordinates = [
                    [
                      [ md['bbox'][0][1], md['bbox'][0][0] ],
                      [ md['bbox'][3][1], md['bbox'][3][0] ],
                      [ md['bbox'][2][1], md['bbox'][2][0] ],
                      [ md['bbox'][1][1], md['bbox'][1][0] ],
                      [ md['bbox'][0][1], md['bbox'][0][0] ]
                    ]
                  ]
        else:
            coordinates = md['union_geojson']['coordinates']
        '''
        geo_type = md['union_geojson']['type']
        coordinates = md['union_geojson']['coordinates']
        print(md['union_geojson'])
        if geo_type == "MultiPolygon":
            print("create_dataset_json : geo_type is %s, selecting first coordinates." %geo_type)
            coordinates = coordinates[0]  
            print("create_dataset_json : coordinates : %s" %coordinates)      
        cord_area = get_area(coordinates[0])

        if not cord_area>0:
            print("creating dataset json. coordinates are not clockwise, reversing it")
            coordinates = [coordinates[0][::-1]]
            print(coordinates)
            cord_area = get_area(coordinates[0])
            if not cord_area>0:
                print("creating dataset json. coordinates are STILL NOT  clockwise")
        else:
            print("creating dataset json. coordinates are already clockwise")

        ds['location'] =  {'type': 'Polygon', 'coordinates': coordinates}

    except Exception as err:
        logger.warn(str(err))
        logger.warn("Traceback: {}".format(traceback.format_exc()))


    ds['starttime'] = md['starttime']
    ds['endtime'] = md['endtime']

    # write out dataset json
    with open(ds_file, 'w') as f:
        json.dump(ds, f, indent=2)

def get_orbit_date(s):
    date = dateutil.parser.parse(s, ignoretz=True)
    date = date.replace(minute=0, hour=12, second=0)
    return date.isoformat()

def get_isoformat_date(s):
    date = dateutil.parser.parse(s, ignoretz=True)
    return date.isoformat()

def get_past_isoformat_date(s, skip_days=0):
    date = dateutil.parser.parse(s, ignoretz=True) - timedelta(days=skip_days)
    return date.isoformat()
    

def get_orbit_file(orbit_dt, platform):
    print("get_orbit_file : %s : %s" %(orbit_dt, platform))
    hits = query_orbit_file(orbit_dt, orbit_dt, platform)
    #print("get_orbit_file : hits : \n%s\n" %hits)
    print("get_orbit_file returns %s result " %len(hits))
    #return hits


    for hit in hits:
        metadata = hit["metadata"]
        
        id = hit['id']
        orbit_platform = metadata["platform"]
        print(orbit_platform)
        if orbit_platform == platform:
            if "urls" in hit:
                urls = hit["urls"]
                url = urls[0]
                if len(urls)>1:
                    for u in urls:
                        if u.startswith('http://') or u.startswith('https://'):
                            url = u
                            break
            else:
                url = metadata["context"]["localize_urls"][0]["url"]

            if url.startswith('s3://'):
                url = metadata["context"]["localize_urls"][0]["url"]
            
            orbit_url = "%s/%s" % (url, hit['metadata']['archive_filename'])
            return True, id, orbit_url,  hit['metadata']['archive_filename']
    return False, None, None, None


def query_orbit_file(starttime, endtime, platform):
    """Query ES for active AOIs that intersect starttime and endtime."""
    print("query_orbit_file: %s, %s, %s" %(starttime, endtime, platform))
    es_index = "grq_*_s1-aux_poeorb"
    query = {
      "query": {
          "bool": {
              "should": [
                {
                  "bool": {
                    "must": [
                  {
                    "range": {
                      "starttime": {
                        "lte": endtime
                      }
                    }
                  },
                  {
                    "range": {
                      "endtime": {
                        "gte": starttime
                      }
                    }
                  },
                  {
                    "match": {
                      "metadata.dataset": "S1-AUX_POEORB"
                    }
                  },
                  {
                    "match": {
                      "metadata.platform": platform
                    }
                  },
                  {
                    "match": {
                      "_type": "S1-AUX_POEORB"
                    }
                  }
                ]  
              }
            }
          ]
        }
      },
      "partial_fields": {
        "partial": {
          "include": [
            "id",
            "starttime",
            "endtime",
            "metadata.platform",
            "metadata.archive_filename",
            "metadata.context.localize_urls",
            "urls"
          ]
        }
      }
    }

    print(query)
    #return query_es(query)


    # filter inactive
    hits = [i['fields']['partial'][0] for i in query_es(query)]
    #print("hits: {}".format(json.dumps(hits, indent=2)))
    #print("aois: {}".format(json.dumps([i['id'] for i in hits])))
    return hits


def get_dates_mission_from_id(id, scene_type):
    """Return day date, slc start date and slc end date."""
    match = None
    if scene_type.upper().startswith("ACQ"):
        match = ACQ_RE.search(id)
    elif scene_type.upper().startswith("SLC"):
        match = SLC_RE.search(id)

    if not match:
        raise RuntimeError("Failed to recognize SLC ID %s." % id)

    day_dt = datetime(int(match.group('start_year')),
                      int(match.group('start_month')),
                      int(match.group('start_day')),
                      0, 0, 0)
    start_dt = datetime(int(match.group('start_year')),
                            int(match.group('start_month')),
                            int(match.group('start_day')),
                            int(match.group('start_hour')),
                            int(match.group('start_min')),
                            int(match.group('start_sec')))
    end_dt = datetime(int(match.group('end_year')),
                          int(match.group('end_month')),
                          int(match.group('end_day')),
                          int(match.group('end_hour')),
                          int(match.group('end_min')),
                          int(match.group('end_sec')))
    mission = match.group('mission')
    return day_dt, start_dt, end_dt, mission

def get_dates_mission_from_metadata(starttime, endtime, platform):
    match_s = DATE_RE.search(starttime)
    if not match_s:
        raise RuntimeError("Failed to recognize starttime %s." % starttime)

    mission = None
    if platform.strip()=='Sentinel-1B':
        mission = 'S1B'
    elif platform.strip()=='Sentinel-1A':
        mission = 'S1A'
    else:
        raise RuntimeError("Unrecognisable Platform : %s" %platform)


    day_dt = datetime(int(match_s.group('year')), int(match_s.group('month')), int(match_s.group('day')),0, 0, 0)
    start_dt = datetime(int(match_s.group('year')),
                            int(match_s.group('month')),
                            int(match_s.group('day')),
                            int(match_s.group('hour')),
                            int(match_s.group('min')),
                            int(match_s.group('sec')))
    match_e = DATE_RE.search(endtime)
    if not match_s:
        raise RuntimeError("Failed to recognize starttime %s." % starttime)
    end_dt = datetime(int(match_e.group('year')),
                            int(match_e.group('month')),
                            int(match_e.group('day')),
                            int(match_e.group('hour')),
                            int(match_e.group('min')),
                            int(match_e.group('sec')))

    return day_dt, start_dt, end_dt, mission


def get_date_from_metadata(mds):
    day_dts = {}
    mission = None 

    for id in mds:
        print(id)
        h = mds[id]
        fields = h["_source"]
        day_dt, start_dt, end_dt, mission = get_dates_mission_from_metadata(fields['starttime'], fields['endtime'], fields['metadata']['platform'])
        print("day_dt : %s, start_dt : %s, end_dt : %s, mission : %s" %(day_dt, start_dt, end_dt, mission))
        day_dts.setdefault(day_dt, []).extend([start_dt, end_dt])
    if len(day_dts) > 1:
        print("Found data for more than 1 day : %s" %day_dts)
        day_dt = sorted(day_dts)[0]
        print("selected : %s" %day_dt)

    all_dts = day_dts[day_dt]
    print("all_dts : %s" %all_dts)

    return day_dt, all_dts, mission

def get_scene_dates_from_metadata(master_mds, slave_mds):
    master_day_dt, master_all_dts, m_mission = get_date_from_metadata(master_mds)
    slave_day_dt, slave_all_dts, s_mission = get_date_from_metadata(slave_mds)
    print("master_day_dt : %s, master_all_dts : %s, m_mission %s: " %(master_day_dt, master_all_dts, m_mission))
    print("slave_day_dt : %s, slave_all_dts : %s, m_mission %s: " %(slave_day_dt, slave_all_dts, m_mission))
    master_all_dts.sort()
    slave_all_dts.sort()
    print("master_day_dt : %s, master_all_dts : %s, m_mission %s: " %(master_day_dt, master_all_dts, m_mission))
    print("slave_day_dt : %s, slave_all_dts : %s, m_mission %s: " %(slave_day_dt, slave_all_dts, m_mission))

    if master_day_dt < slave_day_dt: return master_all_dts[0], slave_all_dts[-1]
    else: return master_all_dts[-1], slave_all_dts[0]

def get_date_from_ids(ids, scene_type):
    '''
    ids : list of acquistion or slc ids
    scene_type : acq or slc
    '''
    mission = None
    
    day_dts = {}
    for id in ids:
        day_dt, start_dt, end_dt, mission = get_dates_mission_from_id(id, scene_type)
        print("day_dt : %s, start_dt : %s, end_dt : %s, mission : %s" %(day_dt, start_dt, end_dt, mission))
        day_dts.setdefault(day_dt, []).extend([start_dt, end_dt])
    if len(day_dts) > 1:
        raise RuntimeError("Found ACQ/SLCs for more than 1 day : %s" %ids)
    all_dts = day_dts[day_dt]
    print("all_dts : %s" %all_dts)
    return day_dt, all_dts, mission


def get_scene_dates_from_ids(master_ids, slave_ids, scene_type):
    '''
    ids : list of acquistion or slc ids
    scene_type : acq or slc
    '''

    master_day_dt, master_all_dts, m_mission = get_date_from_ids(master_ids, scene_type)
    slave_day_dt, slave_all_dts, s_mission = get_date_from_ids(slave_ids, scene_type)
    print("master_day_dt : %s, master_all_dts : %s, m_mission %s: " %(master_day_dt, master_all_dts, m_mission))
    print("slave_day_dt : %s, slave_all_dts : %s, m_mission %s: " %(slave_day_dt, slave_all_dts, m_mission))
    master_all_dts.sort()
    slave_all_dts.sort()

    if master_day_dt < slave_day_dt: return master_all_dts[0], slave_all_dts[-1]
    else: return master_all_dts[-1], slave_all_dts[0]



def get_urls(info):
    """Return list of SLC URLs with preference for S3 URLs."""

    urls = list()
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


