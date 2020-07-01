#!/usr/bin/env python3 
import os, sys, re, requests, json, shutil, traceback, logging, hashlib, math
import math
from glob import glob
from UrlUtils import UrlUtils
from subprocess import check_call, CalledProcessError
from datetime import datetime
import xml.etree.cElementTree as ET
from xml.etree.ElementTree import Element, SubElement
from zipfile import ZipFile
from osgeo import ogr, osr

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_alos2_ifg.log')


BASE_PATH = os.path.dirname(__file__)

IMG_RE=r'IMG-(\w{2})-ALOS(\d{6})(\d{4})-*'

def get_dataset(id, es_index="grq"):
    """Query for existence of dataset by ID."""

    uu = UrlUtils()
    es_url = uu.rest_url
    #es_index = "{}_{}_s1-ifg".format(uu.grq_index_prefix, version)
    es_index = "grq"

    # query
    query = {
      "query": {
        "wildcard": {
          "_id": id
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

    if r.status_code != 200:
        logger.info("Failed to query %s:\n%s" % (es_url, r.text))
        logger.info("query: %s" % json.dumps(query, indent=2))
        logger.info("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    logger.info(result['hits']['total'])
    return result

def get_md5_from_file(file_name):
    '''
    :param file_name: file path to the local SLC file after download
    :return: string, ex. 8e15beebbbb3de0a7dbed50a39b6e41b ALL LOWER CASE
    '''
    hash_md5 = hashlib.md5()
    with open(file_name, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def check_ifg_status(ifg_id, es_index="grq"):

    result = get_dataset(ifg_id, es_index)
    total = result['hits']['total']
    logger.info("check_slc_status : total : %s" %total)
    if total> 0:
        found_id = result['hits']['hits'][0]["_id"]
        raise RuntimeError("IFG already exists : %s" %found_id)

    logger.info("check_slc_status : returning False")
    return False


def get_dataset_by_hash(ifg_hash, es_index="grq"):
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
                    { "term":{"dataset.raw": "S1-GUNW"} }
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

def get_dataset_by_hash_version(ifg_hash, version, es_index="grq"):
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
                    { "term":{"dataset.raw": "S1-GUNW"} },
                    { "term":{"version.raw": version} }
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

def check_ifg_status_by_hash(new_ifg_hash, es_index="grq"):
    es_index="grq_*_s1-gunw"
    result = get_dataset_by_hash(new_ifg_hash, es_index)
    total = result['hits']['total']
    logger.info("check_slc_status_by_hash : total : %s" %total)
    if total>0:
        found_id = result['hits']['hits'][0]["_id"]
        logger.info("Duplicate dataset found: %s" %found_id)
        sys.exit(0)

    logger.info("check_slc_status : returning False")
    return False

def check_ifg_status_by_hash_version(new_ifg_hash, version, es_index="grq"):
    #ces_index="grq_*_s1-gunw"
    result = get_dataset_by_hash_version(new_ifg_hash, version, es_index)
    total = result['hits']['total']
    logger.info("check_slc_status_by_hash : total : %s" %total)
    if total>0:
        found_id = result['hits']['hits'][0]["_id"]
        logger.info("Duplicate dataset found: %s" %found_id)
        sys.exit(0)

    logger.info("check_slc_status : returning False")
    return False

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

def get_date(t):
    try:
        return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ")
    except:
        try:
            return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S")
        except:
            return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%f")

def get_center_time(t1, t2):
    a = get_date(t1)
    b = get_date(t2)
    t = a + (b - a)/2
    return t.strftime("%H%M%S")

'''
def get_time(t):

    if '.' in t:
        t = t.split('.')[0].strip()
    t1 = datetime.strptime(t, '%Y%m%dT%H%M%S')
    t1 = t1.strftime("%Y-%m-%dT%H:%M:%S")
    logger.info(t1)
    return t1
'''

def get_time(t):

    logger.info("get_time(t) : %s" %t)
    t = parser.parse(t).strftime('%Y-%m-%dT%H:%M:%S')
    t1 = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
    logger.info("returning : %s" %t1)
    return t1

def get_time_str(t):

    logger.info("get_time(t) : %s" %t)
    t = parser.parse(t).strftime('%Y-%m-%dT%H:%M:%S')
    return t

def get_date_str(t):

    logger.info("get_time(t) : %s" %t)
    t = parser.parse(t).strftime('%Y-%m-%d')
    return t
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
        print("post_y[0:3] : {}".format(post_y[0:3]))
        if post_y[0:3] == '000':
            post_y = '000'
        else:
            post_y =post_y.ljust(3, '0')
        
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

def get_geocoded_lats(vrt_file):

    ''' return latitudes'''
    import gdal
    import numpy as np

    # extract geo-coded corner coordinates
    ds = gdal.Open(vrt_file)
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    
    # getting the gdal transform and projection
    geoTrans = str(ds.GetGeoTransform())
    projectionRef = str(ds.GetProjection())
    
    lat_arr = list(range(0, rows))
    lats = np.empty((rows,),dtype='float64')
    for py in lat_arr:
        lats[py] = gt[3] + (py * gt[5])

    return lats

def get_pol_value(pp):
    if pp in ("SV", "DV", "VV"): return "VV"
    elif pp in ("DH", "SH", "HH", "HV"): return "HH"
    else: raise RuntimeError("Unrecognized polarization: %s" % pp)

def get_pol_frame_info(slc_filelist):
    pol_arr = [] 
    frame_arr = []
    imgRegex = re.compile(IMG_RE)

    #img_files = glob(os.path.join(slc_dir, "IMG-*"))

    for fl in slc_filelist:
        if fl.startswith("IMG-"):
            mo = imgRegex.search(fl)
            pol_arr.append(get_pol_value(mo.group(1).upper()))
            frame_arr.append(int(mo.group(3)))
            print("{} : {} : {}".format(fl, mo.group(1), mo.group(3)))

    pol_arr = list(set(pol_arr))
    if len(pol_arr)>1:
        print("Error : More than one polarization value in {} : {}".format(slc_dir, pol_arr))
        raise Exception("More than one polarization value in {} : {}".format(slc_dir, pol_arr))

    return pol_arr[0], list(set(frame_arr))     

def fileContainsMsg(file_name, msg):
    return False, None

    with open(file_name, 'r') as f:
        datafile = f.readlines()
    for line in datafile:
        if msg in line:
            # found = True # Not necessary
            return True, line
    return False, None

def checkBurstError(file_name):
    msg = "cannot continue for interferometry applications"

    found, line = fileContainsMsg(file_name, msg)
    if found:
        print("checkBurstError : %s" %line.strip())
        raise RuntimeError(line.strip())
    if not found:
        msg = "Exception: Could not determine a suitable burst offset"
        found, line = fileContainsMsg("alos2app.log", msg)
        if found:
            print("Found Error : %s" %line)
            raise RuntimeError(line.strip())

def updateXml(xml_file):
    logging.info(xml_file)
    path = os.path.split(xml_file)[0]
    tree = ET.parse(xml_file)
    root = tree.getroot()    


    for elem in root:
        if elem.tag == 'property':
            d = elem.attrib
            if 'name' in d.keys() and d['name'] == 'file_name':
       
                for n in elem:
                    if n.tag == 'value':
                        new_value = os.path.join(path, n.text)
                        n.text = new_value
                        logging.info(n.text)
    logging.info(tree)
    tree = ET.ElementTree(root)
    tree.write(xml_file) 

def get_min_mx_lon_lat(bbox):
    lons = []
    lats = []

    for pp in bbox:
        lons.append(pp[0])
        lats.append(pp[1])

    return min(lons), max(lons), min(lats), max(lats)

def get_SNWE_bbox(bbox):

    return get_SNWE(get_min_mx_lon_lat(bbox))

def get_SNWE_complete_bbox(ref_bbox, sec_bbox):

    ref_min_lon, ref_max_lon, ref_min_lat, ref_max_lat = get_min_mx_lon_lat(ref_bbox)
    sec_min_lon, sec_max_lon, sec_min_lat, sec_max_lat = get_min_mx_lon_lat(sec_bbox)

    return get_SNWE(min(ref_min_lon, sec_min_lon), max(ref_max_lon, sec_max_lon), min(ref_min_lat, sec_min_lat), max(ref_max_lat, sec_max_lat))

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

def get_version(dataset_type):
    """Get dataset version."""
    """ dataset_type example : 'S1-GUNW', 'ALOS2' """
    
    ds_ver = None

    DS_VERS_CFG = os.path.normpath(
                      os.path.join(
                          os.path.dirname(os.path.abspath(__file__)),
                          '..', 'conf', 'dataset_versions.json'))
   
    with open(DS_VERS_CFG) as f:
        ds_vers = json.load(f)

    if dataset_type in ds_vers:
        ds_ver = ds_vers[dataset_type]

    return ds_ver

def get_area(coords):
    '''get area of enclosed coordinates- determines clockwise or counterclockwise order'''
    n = len(coords) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += coords[i][1] * coords[j][0]
        area -= coords[j][1] * coords[i][0]
    #area = abs(area) / 2.0
    return area / 2


def create_dataset_json(id, version, met_file, ds_file):
    """Write dataset json."""


    # get metadata
    with open(met_file) as f:
        md = json.load(f)

    # build dataset
    ds = {
        'creation_timestamp': "%sZ" % datetime.utcnow().isoformat(),
        'version': version,
        'label': id
    }

    try:
        '''
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
        '''

        coordinates = md['union_geojson']['coordinates']
        print("coordinates : {}".format(coordinates))
        
        ds['location'] =  {'type': 'Polygon', 'coordinates': coordinates}
        cord_area = get_area(coordinates[0])
        print("cord_area : {}".format(cord_area))
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
        print("create_dataset_json location : %s" %ds['location'])

    except Exception as err:
        print("create_dataset_json: Exception : {}".format(str(err)))
        print("Traceback: {}".format(traceback.format_exc()))


    # set earliest sensing start to starttime and latest sensing stop to endtime
    if isinstance(md['sensing_start'], str):
        ds['starttime'] = md['sensing_start']
    else:
        md['sensing_start'].sort()
        ds['starttime'] = md['sensing_start'][0]

    if isinstance(md['sensing_stop'], str):
        ds['endtime'] = md['sensing_stop']
    else:
        md['sensing_stop'].sort()
        ds['endtime'] = md['sensing_stop'][-1]

    # write out dataset json
    with open(ds_file, 'w') as f:
        json.dump(ds, f, indent=2)

def get_union_polygon(ds_files):
    """Get GeoJSON polygon of union of IFGs."""

    geom_union = None
    for ds_file in ds_files:
         with open(ds_file) as f:
             ds = json.load(f)
             if 'location' not in ds:
                 if 'geometry' in ds:
                     ds['location'] = ds['geometry']
                 else:
                     raise Exception("Location or Geometry NOT found in : {}".format(ds_file))

         geom = ogr.CreateGeometryFromJson(json.dumps(ds['location'], indent=2, sort_keys=True))
         if geom_union is None: geom_union = geom
         else: geom_union = geom_union.Union(geom)
    return json.loads(geom_union.ExportToJson()), geom_union.GetEnvelope()

def get_bool_param(ctx, param):
    """Return bool param from context."""

    if param in ctx and isinstance(ctx[param], bool): return ctx[param]
    return True if ctx.get(param, 'true').strip().lower() == 'true' else False

def get_SNWE(min_lon, max_lon, min_lat, max_lat):
    snwe_arr = []
    dem_S = min_lat
    dem_N = max_lat
    dem_W = min_lon
    dem_E = max_lon

    dem_S = int(math.floor(dem_S))
    dem_N = int(math.ceil(dem_N))
    dem_W = int(math.floor(dem_W))
    dem_E = int(math.ceil(dem_E))

    snwe_arr.append(dem_S)
    snwe_arr.append(dem_N)
    snwe_arr.append(dem_W)
    snwe_arr.append(dem_E)

    return "{} {} {} {}".format(dem_S, dem_N, dem_W, dem_E), snwe_arr

def run_command(cmd):
    cmd_line = " ".join(cmd)
    logging.info("calling : {}".format(cmd_line))
    print("{} : Command : {}".format(os.getcwd(), cmd_line))
    check_call(cmd_line, shell=True)


def move_dem_separate_dir(dir_name):
    move_dem_separate_dir_SRTM(dir_name)
    move_dem_separate_dir_NED(dir_name)

def move_dem_separate_dir_SRTM(dir_name):
    print("move_dem_separate_dir_SRTM : %s" %dir_name)
    create_dir(dir_name)

    move_cmd=["mv", "demLat*", dir_name]
    move_cmd_line=" ".join(move_cmd)
    print("Calling {}".format(move_cmd_line))
    call_noerr(move_cmd_line)

def move_dem_separate_dir_NED(dir_name):
    print("move_dem_separate_dir_NED : %s" %dir_name)
    create_dir(dir_name)
    move_cmd=["mv", "stitched.*", dir_name]
    move_cmd_line=" ".join(move_cmd)
    print("Calling {}".format(move_cmd_line))
    call_noerr(move_cmd_line)

    move_cmd=["mv", "*DEM.vrt", dir_name]
    move_cmd_line=" ".join(move_cmd)
    print("Calling {}".format(move_cmd_line))
    call_noerr(move_cmd_line)

def create_dir(dir_name):
    '''
    if os.path.isdir(dir_name):
        rmdir_cmd=["rm", "-rf", dir_name]
        rmdir_cmd_line=" ".join(rmdir_cmd)
        print("Calling {}".format(rmdir_cmd_line))
        call_noerr(rmdir_cmd_line)
    '''
    if not os.path.isdir(dir_name):
        mkdir_cmd=["mkdir", dir_name]
        mkdir_cmd_line=" ".join(mkdir_cmd)
        print("create_dir : Calling {}".format(mkdir_cmd_line))
        call_noerr(mkdir_cmd_line)

def call_noerr(cmd):
    """Run command and warn if exit status is not 0."""

    try: check_call(cmd, shell=True)
    except Exception as e:
        print("Got exception running {}: {}".format(cmd, str(e)))
        print("Traceback: {}".format(traceback.format_exc()))

def download_dem(SNWE):
    uu = UrlUtils()
    dem_user = uu.dem_u
    dem_pass = uu.dem_p
    srtm3_dem_url = uu.dem_url
    ned1_dem_url = uu.ned1_dem_url
    ned13_dem_url = uu.ned13_dem_url
    dem_type_simple = "SRTM3"
    preprocess_dem_dir="preprocess_dem"
    geocode_dem_dir="geocode_dem"
    dem_type = "SRTM3"
 
    wd = os.getcwd()
    logging.info("Working Directory : {}".format(wd))

    dem_url = srtm3_dem_url
    dem_cmd = [
                "{}/applications/dem.py".format(os.environ['ISCE_HOME']), "-a",
                "stitch", "-b", "{}".format(SNWE),
                "-k", "-s", "1", "-f", "-x", "-c", "-n", dem_user, "-w", dem_pass,
                "-u", dem_url
            ]
    dem_cmd_line = " ".join(dem_cmd)
    logging.info("Calling dem.py: {}".format(dem_cmd_line))
    check_call(dem_cmd_line, shell=True)
    preprocess_dem_file = glob("*.dem.wgs84")[0]

    #cmd= ["rm", "*.zip",  *.dem *.dem.vrt *.dem.xml"
    #check_call(cmd, shell=True)

    preprocess_dem_dir = "{}_{}".format(dem_type_simple, preprocess_dem_dir)


    print("dem_type : %s preprocess_dem_dir : %s" %(dem_type, preprocess_dem_dir))
    if dem_type.startswith("NED"):
        move_dem_separate_dir_NED(preprocess_dem_dir)
    elif dem_type.startswith("SRTM"):
        move_dem_separate_dir_SRTM(preprocess_dem_dir)
    else:
        move_dem_separate_dir(preprocess_dem_dir)

    preprocess_dem_file = os.path.join(preprocess_dem_dir, preprocess_dem_file)
    print("Using Preprocess DEM file: {}".format(preprocess_dem_file))

    #preprocess_dem_file = os.path.join(wd, glob("*.dem.wgs84")[0])
    #logging.info("preprocess_dem_file : {}".format(preprocess_dem_file))

    # fix file path in Preprocess DEM xml
    fix_cmd = [
        "{}/applications/fixImageXml.py".format(os.environ['ISCE_HOME']),
        "-i", preprocess_dem_file, "--full"
    ]
    fix_cmd_line = " ".join(fix_cmd)
    print("Calling fixImageXml.py: {}".format(fix_cmd_line))
    check_call(fix_cmd_line, shell=True)

    preprocess_vrt_file=""
    if dem_type.startswith("SRTM"):
        preprocess_vrt_file = glob(os.path.join(preprocess_dem_dir, "*.dem.wgs84.vrt"))[0]
    elif dem_type.startswith("NED1"):
        preprocess_vrt_file = os.path.join(preprocess_dem_dir, "stitched.dem.vrt")
        print("preprocess_vrt_file : %s"%preprocess_vrt_file)
    else: raise RuntimeError("Unknown dem type %s." % dem_type)

    if not os.path.isfile(preprocess_vrt_file):
        print("%s does not exists. Exiting")
    
    preprocess_dem_xml = glob(os.path.join(preprocess_dem_dir, "*.dem.wgs84.xml"))[0]
    logging.info("preprocess_dem_xml : {}".format(preprocess_dem_xml))
    updateXml(preprocess_dem_xml)

    geocode_dem_dir = os.path.join(preprocess_dem_dir, "Coarse_{}_preprocess_dem".format(dem_type_simple))
    create_dir(geocode_dem_dir)

    '''
    os.chdir(geocode_dem_dir)
    dem_cmd = [
                "/usr/local/isce/isce/applications/dem.py", "-a",
                "stitch", "-b", "{}".format(SNWE),
                "-k", "-s", "3", "-f", "-c", "-x", "-n", dem_user, "-w", dem_pass,
                "-u", dem_url
            ]
    dem_cmd_line = " ".join(dem_cmd)
    logging.info("Calling dem.py: {}".format(dem_cmd_line))
    check_call(dem_cmd_line, shell=True)
    '''
    
    
    dem_cmd = [
        "{}/applications/downsampleDEM.py".format(os.environ['ISCE_HOME']), "-i",
        "{}".format(preprocess_vrt_file), "-rsec", "3"
    ]
    dem_cmd_line = " ".join(dem_cmd)
    print("Calling downsampleDEM.py: {}".format(dem_cmd_line))
    check_call(dem_cmd_line, shell=True)
    geocode_dem_file = ""
    

    print("geocode_dem_dir : {}".format(geocode_dem_dir))
    if dem_type.startswith("SRTM"):
        geocode_dem_file = glob(os.path.join(geocode_dem_dir, "*.dem.wgs84"))[0]
    elif dem_type.startswith("NED1"):
        geocode_dem_file = os.path.join(geocode_dem_dir, "stitched.dem")
    print("Using Geocode DEM file: {}".format(geocode_dem_file))

    checkBurstError("isce.log")

    # fix file path in Geocoding DEM xml
    fix_cmd = [
        "{}/applications/fixImageXml.py".format(os.environ['ISCE_HOME']),
        "-i", geocode_dem_file, "--full"
    ]
    fix_cmd_line = " ".join(fix_cmd)
    print("Calling fixImageXml.py: {}".format(fix_cmd_line))
    check_call(fix_cmd_line, shell=True)



    geocode_dem_xml = glob(os.path.join(geocode_dem_dir, "*.dem.wgs84.xml"))[0]


    os.chdir(wd)
    cmd= ["pwd"]
    cmd_line = " ".join(cmd)
    check_call(cmd_line, shell=True)

    return preprocess_dem_file, geocode_dem_file, preprocess_dem_xml, geocode_dem_xml


def get_zip_contents(file_name):
    file_list = []
    my_zip = ZipFile(file_name)
    for file in my_zip.namelist():
        file_list.append(my_zip.getinfo(file).filename)
        print(my_zip.getinfo(file).filename)
    return file_list

def extract_partial_zip_files(zip_file, target_dir, filters):

    print("extract_partial_zip_files : {} : {}: {}".format(zip_file, target_dir, filters))
    my_zip = ZipFile(zip_file)
    for file in my_zip.namelist():
        file_name = my_zip.getinfo(file).filename
        if file_name.startswith(tuple(set(filters))):
            my_zip.extract(file_name, target_dir)

def unzip_slcs(slcs, filters = []):
    for k, v in slcs.items():
        logging.info("Unzipping {} in {}".format(v, k))
        if len(filters) > 0:
            extract_partial_zip_files(v, k, filters)
        else:
            with ZipFile(v, 'r') as zf:
                zf.extractall(k)
        ''' 
        logging.info("Removing {}.".format(v))
        try: os.unlink(v)
        except: pass
        '''
        
def change_dir(wd):
    os.chdir(wd)
    cmd= ["pwd"]
    run_command(cmd)

def run_command(cmd_array):
    cmd_line = " ".join(cmd_array)
    logging.info("cmd_line : {}".format(cmd_line))
    check_call(cmd_line, shell=True)


