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
from create_input_xml import create_input_xml
from osgeo import ogr, osr

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_alos2_ifg.log')


BASE_PATH = os.path.dirname(__file__)

IMG_RE=r'IMG-(\w{2})-ALOS(\d{6})(\d{4})-*'

def get_pol_value(pp):
    if pp in ("SV", "DV", "VV"): return "VV"
    elif pp in ("DH", "SH", "HH", "HV"): return "HH"
    else: raise RuntimeError("Unrecognized polarization: %s" % pp)

def get_pol_frame_info(slc_dir):
    pol_arr = [] 
    frame_arr = []
    imgRegex = re.compile(IMG_RE)

    img_files = glob(os.path.join(slc_dir, "IMG-*"))

    for img_f in img_files:
        mo = imgRegex.search(img_f)
        pol_arr.append(get_pol_value(mo.group(1).upper()))
        frame_arr.append(int(mo.group(3)))
        print("{} : {} : {}".format(img_f, mo.group(1), mo.group(3)))

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
        logger.info("checkBurstError : %s" %line.strip())
        raise RuntimeError(line.strip())
    if not found:
        msg = "Exception: Could not determine a suitable burst offset"
        found, line = fileContainsMsg("alos2app.log", msg)
        if found:
            logger.info("Found Error : %s" %line)
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
                          '..', '..', 'conf', 'dataset_versions.json'))
   
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
        logger.info("create_dataset_json : met['bbox']: %s" %md['bbox'])
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
    
        cord_area = get_area(coordinates[0])
        if not cord_area>0:
            logger.info("creating dataset json. coordinates are not clockwise, reversing it")
            coordinates = [coordinates[0][::-1]]
            logger.info(coordinates)
            cord_area = get_area(coordinates[0])
            if not cord_area>0:
                logger.info("creating dataset json. coordinates are STILL NOT  clockwise")
        else:
            logger.info("creating dataset json. coordinates are already clockwise")

        ds['location'] =  {'type': 'Polygon', 'coordinates': coordinates}
        logger.info("create_dataset_json location : %s" %ds['location'])

    except Exception as err:
        logger.info("create_dataset_json: Exception : ")
        logger.warn(str(err))
        logger.warn("Traceback: {}".format(traceback.format_exc()))


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
    check_call(cmd_line, shell=True)


def move_dem_separate_dir(dir_name):
    move_dem_separate_dir_SRTM(dir_name)
    move_dem_separate_dir_NED(dir_name)

def move_dem_separate_dir_SRTM(dir_name):
    logger.info("move_dem_separate_dir_SRTM : %s" %dir_name)
    create_dir(dir_name)

    move_cmd=["mv", "demLat*", dir_name]
    move_cmd_line=" ".join(move_cmd)
    logger.info("Calling {}".format(move_cmd_line))
    call_noerr(move_cmd_line)

def move_dem_separate_dir_NED(dir_name):
    logger.info("move_dem_separate_dir_NED : %s" %dir_name)
    create_dir(dir_name)
    move_cmd=["mv", "stitched.*", dir_name]
    move_cmd_line=" ".join(move_cmd)
    logger.info("Calling {}".format(move_cmd_line))
    call_noerr(move_cmd_line)

    move_cmd=["mv", "*DEM.vrt", dir_name]
    move_cmd_line=" ".join(move_cmd)
    logger.info("Calling {}".format(move_cmd_line))
    call_noerr(move_cmd_line)

def create_dir(dir_name):
    '''
    if os.path.isdir(dir_name):
        rmdir_cmd=["rm", "-rf", dir_name]
        rmdir_cmd_line=" ".join(rmdir_cmd)
        logger.info("Calling {}".format(rmdir_cmd_line))
        call_noerr(rmdir_cmd_line)
    '''
    if not os.path.isdir(dir_name):
        mkdir_cmd=["mkdir", dir_name]
        mkdir_cmd_line=" ".join(mkdir_cmd)
        logger.info("create_dir : Calling {}".format(mkdir_cmd_line))
        call_noerr(mkdir_cmd_line)

def call_noerr(cmd):
    """Run command and warn if exit status is not 0."""

    try: check_call(cmd, shell=True)
    except Exception as e:
        logger.warn("Got exception running {}: {}".format(cmd, str(e)))
        logger.warn("Traceback: {}".format(traceback.format_exc()))

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


    logger.info("dem_type : %s preprocess_dem_dir : %s" %(dem_type, preprocess_dem_dir))
    if dem_type.startswith("NED"):
        move_dem_separate_dir_NED(preprocess_dem_dir)
    elif dem_type.startswith("SRTM"):
        move_dem_separate_dir_SRTM(preprocess_dem_dir)
    else:
        move_dem_separate_dir(preprocess_dem_dir)

    preprocess_dem_file = os.path.join(preprocess_dem_dir, preprocess_dem_file)
    logger.info("Using Preprocess DEM file: {}".format(preprocess_dem_file))

    #preprocess_dem_file = os.path.join(wd, glob("*.dem.wgs84")[0])
    #logging.info("preprocess_dem_file : {}".format(preprocess_dem_file))

    # fix file path in Preprocess DEM xml
    fix_cmd = [
        "{}/applications/fixImageXml.py".format(os.environ['ISCE_HOME']),
        "-i", preprocess_dem_file, "--full"
    ]
    fix_cmd_line = " ".join(fix_cmd)
    logger.info("Calling fixImageXml.py: {}".format(fix_cmd_line))
    check_call(fix_cmd_line, shell=True)

    preprocess_vrt_file=""
    if dem_type.startswith("SRTM"):
        preprocess_vrt_file = glob(os.path.join(preprocess_dem_dir, "*.dem.wgs84.vrt"))[0]
    elif dem_type.startswith("NED1"):
        preprocess_vrt_file = os.path.join(preprocess_dem_dir, "stitched.dem.vrt")
        logger.info("preprocess_vrt_file : %s"%preprocess_vrt_file)
    else: raise RuntimeError("Unknown dem type %s." % dem_type)

    if not os.path.isfile(preprocess_vrt_file):
        logger.info("%s does not exists. Exiting")
    
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
    logger.info("Calling downsampleDEM.py: {}".format(dem_cmd_line))
    check_call(dem_cmd_line, shell=True)
    geocode_dem_file = ""
    

    logger.info("geocode_dem_dir : {}".format(geocode_dem_dir))
    if dem_type.startswith("SRTM"):
        geocode_dem_file = glob(os.path.join(geocode_dem_dir, "*.dem.wgs84"))[0]
    elif dem_type.startswith("NED1"):
        geocode_dem_file = os.path.join(geocode_dem_dir, "stitched.dem")
    logger.info("Using Geocode DEM file: {}".format(geocode_dem_file))

    checkBurstError("isce.log")

    # fix file path in Geocoding DEM xml
    fix_cmd = [
        "{}/applications/fixImageXml.py".format(os.environ['ISCE_HOME']),
        "-i", geocode_dem_file, "--full"
    ]
    fix_cmd_line = " ".join(fix_cmd)
    logger.info("Calling fixImageXml.py: {}".format(fix_cmd_line))
    check_call(fix_cmd_line, shell=True)



    geocode_dem_xml = glob(os.path.join(geocode_dem_dir, "*.dem.wgs84.xml"))[0]


    os.chdir(wd)
    cmd= ["pwd"]
    cmd_line = " ".join(cmd)
    check_call(cmd_line, shell=True)

    return preprocess_dem_file, geocode_dem_file, preprocess_dem_xml, geocode_dem_xml

def unzip_slcs(slcs):
    for k, v in slcs.items():
        logging.info("Unzipping {} in {}".format(v, k))
        with ZipFile(v, 'r') as zf:
            zf.extractall(k)
        logging.info("Removing {}.".format(v))
        #try: os.unlink(v)
        #except: pass


