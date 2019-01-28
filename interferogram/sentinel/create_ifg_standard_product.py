#!/usr/bin/env python3 
import os, sys, re, requests, json, shutil, traceback, logging, hashlib, math
from itertools import chain
from zipfile import ZipFile
from subprocess import check_call, CalledProcessError
from glob import glob
from lxml.etree import parse
import numpy as np
from datetime import datetime
from osgeo import ogr, gdal

from isceobj.Image.Image import Image
from utils.UrlUtils_standard_product import UrlUtils
from utils.imutils import get_image, get_size, crop_mask
from check_interferogram import check_int
from create_input_xml_standard_product import create_input_xml

import os
from scipy.constants import c
import isce
from iscesys.Component.ProductManager import ProductManager as PM


gdal.UseExceptions() # make GDAL raise python exceptions


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_ifg')


BASE_PATH = os.path.dirname(__file__)


KILAUEA_DEM_XML = "https://aria-alt-dav.jpl.nasa.gov/repository/products/kilauea/dem_kilauea.dem.xml"
KILAUEA_DEM = "https://aria-alt-dav.jpl.nasa.gov/repository/products/kilauea/dem_kilauea.dem"


MISSION_RE = re.compile(r'^(S1\w)_')
POL_RE = re.compile(r'^S1\w_IW_SLC._1S(\w{2})_')
IFG_ID_SP_TMPL = "S1-{}-{}-{:03d}-tops-{}_{}-{}-{}-PP-{}-{}"
IFG_ID_SP_MERGED_TMPL = "S1-MERGED-{}-{}-{:03d}-tops-{}_{}-{}-{}-PP-{}-{}"

def update_met_key(met_md, old_key, new_key):
    try:
        if old_key in met_md:
            met_md[new_key] = met_md.pop(old_key)
    except Exception as err:
        print("Failed to replace %s from met file with %s. Error : %s" %(old_key, new_key, str(err)))

    return met_md

def delete_met_data(met_md, old_key):
    try:
        if old_key in met_md:
            del met_md[old_key] 
    except Exception as err:
        print("Failed to delete %s from met file. Error : %s" %(old_key,  str(err)))

    return met_md



def update_met(md):


    #Keys to update
    md = update_met_key(md, "sensingStart", "sensing_start")
    md = update_met_key(md, "trackNumber", "track_number")
    md = update_met_key(md, "imageCorners", "image_corners")
    md = update_met_key(md, "lookDirection", "look_direction")
    md = update_met_key(md, "inputFile", "input_file")
    md = update_met_key(md, "startingRange", "starting_range")
    md = update_met_key(md, "latitudeIndexMax", "latitude_index_max")
    md = update_met_key(md, "frameID", "frame_id")
    md = update_met_key(md, "frameNumber", "frame_number")
    md = update_met_key(md, "beamID", "beam_id")
    md = update_met_key(md, "orbitNumber", "orbit_number")
    md = update_met_key(md, "latitudeIndexMin", "latitude_index_min")
    md = update_met_key(md, "beamMode", "beam_mode")
    md = update_met_key(md, "orbitRepeat", "orbit_repeat")
    md = update_met_key(md, "perpendicularBaseline", "perpendicular_baseline")
    md = update_met_key(md, "frameName", "frame_name")
    md = update_met_key(md, "sensingStop", "sensing_stop")
    md = update_met_key(md, "parallelBaseline", "parallel_baseline")
    md = update_met_key(md, "direction", "orbit_direction")


    #keys to delete
    md = delete_met_data(md, "swath")
    md = delete_met_data(md, "spacecraftName")
    md = delete_met_data(md, "reference")
    
    return md

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


def get_updated_met(metjson):
    new_met = {}
    return new_met

def get_tops_subswath_xml(masterdir):
    ''' 
        Find all available IW[1-3].xml files
    '''

    logger.info("get_tops_subswath_xml from : %s" %masterdir)

    masterdir = os.path.abspath(masterdir)
    IWs = glob(os.path.join(masterdir,'IW*.xml'))
    if len(IWs)<1:
        raise Exception("Could not find a IW*.xml file in " + masterdir)

    return IWs

def read_isce_product(xmlfile):
    logger.info("read_isce_product: %s" %xmlfile)

    # check if the file does exist
    check_file_exist(xmlfile)

    # loading the xml file with isce
    pm = PM()
    pm.configure()
    obj = pm.loadProduct(xmlfile)
    return obj

def check_file_exist(infile):
    logger.info("check_file_exist : %s" %infile)
    if not os.path.isfile(infile):
        raise Exception(infile + " does not exist")
    else:
        logger.info("%s Exists" %infile)


def get_tops_metadata(masterdir):

    logger.info("get_tops_metadata from : %s" %masterdir)
    # get a list of avialble xml files for IW*.xml
    IWs = get_tops_subswath_xml(masterdir)
    # append all swaths togheter
    frames=[]
    for IW  in IWs:
        logger.info("get_tops_metadata processing : %s" %IW)
        obj = read_isce_product(IW)
        frames.append(obj)

    output={}
    dt = min(frame.sensingStart for frame in frames)
    output['sensingStart'] =  dt.isoformat('T') + 'Z'
    logger.info(dt)
    dt = max(frame.sensingStop for frame in frames)
    output['sensingStop'] = dt.isoformat('T') + 'Z'
    logger.info(dt)
    return output


def get_version():
    """Get dataset version."""

    DS_VERS_CFG = os.path.normpath(
                      os.path.join(
                          os.path.dirname(os.path.abspath(__file__)),
                          '..', '..', 'conf', 'dataset_versions.json'))
    with open(DS_VERS_CFG) as f:
        ds_vers = json.load(f)
    return ds_vers['S1-IFG']


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
         geom = ogr.CreateGeometryFromJson(json.dumps(ds['location'], indent=2, sort_keys=True))
         if geom_union is None: geom_union = geom
         else: geom_union = geom_union.Union(geom)
    return json.loads(geom_union.ExportToJson()), geom_union.GetEnvelope()


def get_times(ds_files):
    """Get starttimes and endtimes."""

    starttimes = []
    endtimes = []
    for ds_file in ds_files:
         with open(ds_file) as f:
             ds = json.load(f)
         starttimes.append(ds['starttime'])
         endtimes.append(ds['endtime'])
    return starttimes, endtimes


def create_stitched_dataset_json(id, version, ds_files, ds_json_file):
    """Create HySDS dataset json file."""

    # get union polygon
    location, env = get_union_polygon(ds_files)
    logger.info("union polygon: {}.".format(json.dumps(location, indent=2, sort_keys=True)))

    # get starttime and endtimes
    starttimes, endtimes = get_times(ds_files)
    starttimes.sort()
    endtimes.sort()
    starttime = starttimes[0]
    endtime = endtimes[-1]

    # build dataset
    ds = {
        'creation_timestamp': "%sZ" % datetime.utcnow().isoformat(),
        'version': version,
        'label': id,
        'location': location,
        'starttime': starttime,
        'endtime': endtime,
    }

    # write out dataset json
    with open(ds_json_file, 'w') as f:
        json.dump(ds, f, indent=2)

    # return envelope and times
    return env, starttime, endtime


def create_stitched_met_json(id, version, env, starttime, endtime, met_files, met_json_file):
    """Create HySDS met json file."""

    # build met
    bbox = [
        [ env[3], env[0] ],
        [ env[3], env[1] ],
        [ env[2], env[1] ],
        [ env[2], env[0] ],
    ]
    met = {
        'product_type': 'interferogram',
        'master_scenes': [],
        'refbbox': [],
        'esd_threshold': [],
        'frameID': [],
        'temporal_span': [],
        #'swath': [1, 2, 3],
        'trackNumber': [],
        'archive_filename': id,
        'dataset_type': 'slc',
        'tile_layers': [ 'amplitude', 'interferogram' ],
        'latitudeIndexMin': int(math.floor(env[2] * 10)),
        'latitudeIndexMax': int(math.ceil(env[3] * 10)),
        'parallelBaseline': [],
        'url': [],
        'doppler': [],
        'version': [],
        'slave_scenes': [],
        'orbit_type': [],
        'spacecraftName': [],
        'frameNumber': None,
        'reference': None,
        'bbox': bbox,
        'ogr_bbox': [[x, y] for y, x in bbox],
        'orbitNumber': [],
        'inputFile': '"sentinel.ini',
        'perpendicularBaseline': [],
        'orbitRepeat': [],
        'sensingStop': endtime,
        'polarization': [],
        'scene_count': 1,
        'beamID': None,
        'sensor': [],
        'lookDirection': [],
        'platform': [],
        'startingRange': [],
        'frameName': [],
        'tiles': True,
        'sensingStart': starttime,
        'beamMode': [],
        'imageCorners': [],
        'direction': [],
        'prf': [],
        'range_looks': [],
        'dem_type': None,
        'filter_strength': [],
	'azimuth_looks': [],
        "sha224sum": hashlib.sha224(str.encode(os.path.basename(met_json_file))).hexdigest(),
    }

    # collect values
    set_params = ('master_scenes', 'esd_threshold', 'frameID', 'parallelBaseline',
                  'doppler', 'version', 'slave_scenes', 'orbit_type', 'spacecraftName',
                  'orbitNumber', 'perpendicularBaseline', 'orbitRepeat', 'polarization', 
                  'sensor', 'lookDirection', 'platform', 'startingRange',
                  'beamMode', 'direction', 'prf', 'azimuth_looks')
    single_params = ('temporal_span', 'trackNumber', 'dem_type')
    list_params = ('platform', 'perpendicularBaseline', 'parallelBaseline', 'range_looks','filter_strength')
    mean_params = ('perpendicularBaseline', 'parallelBaseline')
    for i, met_file in enumerate(met_files):
        with open(met_file) as f:
            md = json.load(f)
        for param in set_params:
            #logger.info("param: {}".format(param))
            if isinstance(md[param], list):
                met[param].extend(md[param])
            else:
                met[param].append(md[param])
        if i == 0:
            for param in single_params:
                met[param] = md[param]
        met['scene_count'] += 1
    for param in set_params:
        tmp_met = list(set(met[param]))
        if param in list_params:
            met[param] = tmp_met
        else:
            met[param] = tmp_met[0] if len(tmp_met) == 1 else tmp_met
    for param in mean_params:
        met[param] = np.mean(met[param])

    # write out dataset json
    with open(met_json_file, 'w') as f:
        json.dump(met, f, indent=2)

def ifg_exists(es_url, es_index, id):
    """Check interferogram exists in GRQ."""

    total, id = check_int(es_url, es_index, id)
    if total > 0: return True
    return False


def download_file(url, outdir='.', session=None):
    """Download file to specified directory."""

    if session is None: session = requests.session()
    path = os.path.join(outdir, os.path.basename(url))
    logger.info('Downloading URL: {}'.format(url))
    r = session.get(url, stream=True, verify=False)
    try:
        val = r.raise_for_status()
        success = True
    except:
        success = False
    if success:
        with open(path,'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
    return success


def get_polarization(id):
    """Return polarization."""

    match = POL_RE.search(id)
    if not match:
        raise RuntimeError("Failed to extract polarization from %s" % id)
    pp = match.group(1)
    if pp in ("SV", "DV"): return "vv"
    elif pp == "DH": return "hv"
    elif pp == "SH": return "hh"
    else: raise RuntimeError("Unrecognized polarization: %s" % pp)


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


def main():
    """HySDS PGE wrapper for TopsInSAR interferogram generation."""

    # save cwd (working directory)
    complete_start_time=datetime.now()
    logger.info("TopsApp End Time : {}".format(complete_start_time))
    cwd = os.getcwd()

    # get context
    ctx_file = os.path.abspath('_context.json')
    if not os.path.exists(ctx_file):
        raise RuntimeError("Failed to find _context.json.")
    with open(ctx_file) as f:
        ctx = json.load(f)
    logger.info("ctx: {}".format(json.dumps(ctx, indent=2)))

    #Pull topsApp configs
    ctx['azimuth_looks'] = ctx.get("context", {}).get("azimuth_looks", 3)
    ctx['range_looks'] = ctx.get("context", {}).get("range_looks", 7)
    
    ctx['swathnum'] = None
    # stitch all subswaths?
    ctx['stitch_subswaths_xt'] = False
    if ctx['swathnum'] is None:
        ctx['stitch_subswaths_xt'] = True
        ctx['swathnum'] = [1, 2, 3]
        # use default azimuth and range looks for cross-swath stitching
        ctx['azimuth_looks'] = ctx.get("context", {}).get("azimuth_looks", 7)
        ctx['range_looks'] = ctx.get("context", {}).get("range_looks", 19)
    logger.info("Using azimuth_looks of %d and range_looks of %d" % (ctx['azimuth_looks'],ctx['range_looks']))

    if ctx['stitch_subswaths_xt'] == True:
         logger.info("STITCHED SWATHS")
    else:
        logger.info("SINGLE SWATH")

    ctx['filter_strength'] = ctx.get("context", {}).get("filter_strength", 0.5)
    logger.info("Using filter_strength of %f" % ctx['filter_strength'])

    # unzip SAFE dirs
    master_safe_dirs = []
    for i in ctx['master_zip_file']:
        logger.info("Unzipping {}.".format(i))
        with ZipFile(i, 'r') as zf:
            zf.extractall()
        logger.info("Removing {}.".format(i))
        try: os.unlink(i)
        except: pass
        master_safe_dirs.append(i.replace(".zip", ".SAFE"))
    slave_safe_dirs = []
    for i in ctx['slave_zip_file']:
        logger.info("Unzipping {}.".format(i))
        with ZipFile(i, 'r') as zf:
            zf.extractall()
        logger.info("Removing {}.".format(i))
        try: os.unlink(i)
        except: pass
        slave_safe_dirs.append(i.replace(".zip", ".SAFE"))

    # get polarization values
    master_pol = get_polarization(master_safe_dirs[0])
    slave_pol = get_polarization(slave_safe_dirs[0])
    if master_pol == slave_pol:
        match_pol = master_pol
    else:
        match_pol = "{{{},{}}}".format(master_pol, slave_pol)

    # get union bbox
    logger.info("Determining envelope bbox from SLC swaths.")
    bbox_json = "bbox.json"
    
    if ctx['stitch_subswaths_xt']:
        logger.info("stitch_subswaths_xt is True")
        bbox_cmd_tmpl = "{}/get_union_bbox.sh -o {} *.SAFE/annotation/s1?-iw?-slc-{}-*.xml"
        check_call(bbox_cmd_tmpl.format(BASE_PATH, bbox_json,
                                    match_pol), shell=True)
    else:
        logger.info("stitch_subswaths_xt is False. Processing for swathnum : %s" %ctx['swathnum'])
        bbox_cmd_tmpl = "{}/get_union_bbox.sh -o {} *.SAFE/annotation/s1?-iw{}-slc-{}-*.xml"
        check_call(bbox_cmd_tmpl.format(BASE_PATH, bbox_json, ctx['swathnum'],
                                    match_pol), shell=True)
    
    with open(bbox_json) as f:
        bbox = json.load(f)['envelope']
    logger.info("bbox: {}".format(bbox))

    # get id base
    id_base = ctx['id']
    logger.info("Product base ID: {}".format(id_base))
    
    # get dataset version and set dataset ID
    version = get_version()
    id = "{}-{}-{}".format(id_base, version, re.sub("[^a-zA-Z0-9_]", "_", ctx.get("context",{})
                                               .get("dataset_tag","standard")))

    # get endpoint configurations
    uu = UrlUtils()
    es_url = uu.rest_url
    es_index = "{}_{}_s1-ifg".format(uu.grq_index_prefix, version)

    # check if interferogram already exists
    logger.info("GRQ url: {}".format(es_url))
    logger.info("GRQ index: {}".format(es_index))
    logger.info("Product ID for version {}: {}".format(version, id))
    '''
    if ifg_exists(es_url, es_index, id):
        logger.info("{} interferogram for {}".format(version, id_base) +
                    " was previously generated and exists in GRQ database.")

        # cleanup SAFE dirs
        for i in chain(master_safe_dirs, slave_safe_dirs):
            logger.info("Removing {}.".format(i))
            try: shutil.rmtree(i)
            except: pass
        return 0
    '''
    # get DEM configuration
    dem_type = ctx.get("context", {}).get("dem_type", "SRTM+v3")
    dem_type_simple = "SRTM"
    dem_url = uu.dem_url
    srtm3_dem_url = uu.srtm3_dem_url
    ned1_dem_url = uu.ned1_dem_url
    ned13_dem_url = uu.ned13_dem_url
    dem_user = uu.dem_u
    dem_pass = uu.dem_p

    preprocess_dem_dir="preprocess_dem"
    geocode_dem_dir="geocode_dem"

    # download project specific preprocess DEM
    if 'kilauea' in ctx['project']:
        s = requests.session()
        s.auth = (dem_user, dem_pass)
        download_file(KILAUEA_DEM_XML, session=s)
        download_file(KILAUEA_DEM, session=s)
        dem_type_simple = "KILAUEA"
        preprocess_dem_file = os.path.basename(KILAUEA_DEM)
    else:
        # get DEM bbox
        dem_S, dem_N, dem_W, dem_E = bbox
        dem_S = int(math.floor(dem_S))
        dem_N = int(math.ceil(dem_N))
        dem_W = int(math.floor(dem_W))
        dem_E = int(math.ceil(dem_E))
        

        if dem_type.startswith("SRTM"):
            dem_type_simple = "SRTM"
            if dem_type.startswith("SRTM3"):
                dem_url = srtm3_dem_url
                dem_type_simple = "SRTM3"
  
            dem_cmd = [
                "{}/applications/dem.py".format(os.environ['ISCE_HOME']), "-a",
                "stitch", "-b", "{} {} {} {}".format(dem_S, dem_N, dem_W, dem_E),
                "-r", "-s", "1", "-f", "-x", "-c", "-n", dem_user, "-w", dem_pass,
                "-u", dem_url
            ]
            dem_cmd_line = " ".join(dem_cmd)
            logger.info("Calling dem.py: {}".format(dem_cmd_line))
            check_call(dem_cmd_line, shell=True)
            preprocess_dem_file = glob("*.dem.wgs84")[0]
            
        else:
            if dem_type == "NED1": 
                dem_url = ned1_dem_url
                dem_type_simple = "NED1"
            elif dem_type.startswith("NED13"): 
                dem_url = ned13_dem_url
                dem_type_simple = "NED13"
            else: raise RuntimeError("Unknown dem type %s." % dem_type)
            if dem_type == "NED13-downsampled": downsample_option = "-d 33%"
            else: downsample_option = ""
            dem_S = dem_S - 1 if dem_S > -89 else dem_S
            dem_N = dem_N + 1 if dem_N < 89 else dem_N
            dem_W = dem_W - 1 if dem_W > -179 else dem_W
            dem_E = dem_E + 1 if dem_E < 179 else dem_E
            dem_cmd = [
                "{}/ned_dem.py".format(BASE_PATH), "-a",
                "stitch", "-b", "{} {} {} {}".format(dem_S, dem_N, dem_W, dem_E),
                downsample_option, "-u", dem_user, "-p", dem_pass, dem_url
            ]
            dem_cmd_line = " ".join(dem_cmd)
            logger.info("Calling ned_dem.py: {}".format(dem_cmd_line))
            check_call(dem_cmd_line, shell=True)
            preprocess_dem_file = "stitched.dem"
    logger.info("Preprocess DEM file: {}".format(preprocess_dem_file))

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

    # fix file path in Preprocess DEM xml
    fix_cmd = [
        "{}/applications/fixImageXml.py".format(os.environ['ISCE_HOME']),
        "-i", preprocess_dem_file, "--full"
    ]
    fix_cmd_line = " ".join(fix_cmd)
    logger.info("Calling fixImageXml.py: {}".format(fix_cmd_line))
    check_call(fix_cmd_line, shell=True)
    
    '''
    geocode_dem_url = srtm3_dem_url
    dem_cmd = [
        "{}/applications/dem.py".format(os.environ['ISCE_HOME']), "-a",
        "stitch", "-b", "{} {} {} {}".format(dem_S, dem_N, dem_W, dem_E),
        "-r", "-s", "3", "-f", "-x", "-c", "-n", dem_user, "-w", dem_pass,
        "-u", geocode_dem_url
    ]
    dem_cmd_line = " ".join(dem_cmd)
    logger.info("Calling dem.py: {}".format(dem_cmd_line))
    check_call(dem_cmd_line, shell=True)
    geocode_dem_file = glob("*.dem.wgs84")[0]
    
    move_dem_separate_dir(geocode_dem_dir)
    geocode_dem_file = os.path.join(geocode_dem_dir, geocode_dem_file)
    logger.info("Using Geocode DEM file: {}".format(geocode_dem_file))
    '''

    preprocess_vrt_file=""
    if dem_type.startswith("SRTM"):
        preprocess_vrt_file = glob(os.path.join(preprocess_dem_dir, "*.dem.wgs84.vrt"))[0]
    elif dem_type.startswith("NED1"):
        preprocess_vrt_file = os.path.join(preprocess_dem_dir, "combinedDEM.vrt")
        logger.info("preprocess_vrt_file : %s"%preprocess_vrt_file)
    else: raise RuntimeError("Unknown dem type %s." % dem_type)

    if not os.path.isfile(preprocess_vrt_file):
        logger.info("%s does not exists. Exiting")
    
    geocode_dem_dir = os.path.join(preprocess_dem_dir, "Coarse_{}_preprocess_dem".format(dem_type_simple))
    create_dir(geocode_dem_dir)

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
        geocode_dem_file = os.path.join(geocode_dem_dir, "combinedDEM")
    logger.info("Using Geocode DEM file: {}".format(geocode_dem_file))


    # fix file path in Geocoding DEM xml
    fix_cmd = [
        "{}/applications/fixImageXml.py".format(os.environ['ISCE_HOME']),
        "-i", geocode_dem_file, "--full"
    ]
    fix_cmd_line = " ".join(fix_cmd)
    logger.info("Calling fixImageXml.py: {}".format(fix_cmd_line))
    check_call(fix_cmd_line, shell=True)
    
    # download auciliary calibration files
    aux_cmd = [
        #"{}/fetchCal.py".format(BASE_PATH), "-o", "aux_cal"
        "{}/fetchCalES.py".format(BASE_PATH), "-o", "aux_cal"
    ]
    aux_cmd_line = " ".join(aux_cmd)
    #logger.info("Calling fetchCal.py: {}".format(aux_cmd_line))
    logger.info("Calling fetchCalES.py: {}".format(aux_cmd_line))
    check_call(aux_cmd_line, shell=True)
        
    # create initial input xml
    do_esd = True
    esd_coh_th = 0.85
    xml_file = "topsApp.xml"
    create_input_xml(os.path.join(BASE_PATH, 'topsApp_standard_product.xml.tmpl'), xml_file,
                     str(master_safe_dirs), str(slave_safe_dirs), 
                     ctx['master_orbit_file'], ctx['slave_orbit_file'],
                     master_pol, slave_pol, preprocess_dem_file, geocode_dem_file,
                     "1, 2, 3" if ctx['stitch_subswaths_xt'] else ctx['swathnum'],
                     ctx['azimuth_looks'], ctx['range_looks'], ctx['filter_strength'],
                     "{} {} {} {}".format(*bbox), "True", do_esd,
                     esd_coh_th)

    #get the time before stating topsApp.py
    topsApp_start_time=datetime.now()
    logger.info("TopsApp Start Time : {}".format(topsApp_start_time))

    # run topsApp to prepesd step
    topsapp_cmd = [
        "topsApp.py", "--steps", "--end=prepesd",
    ]
    topsapp_cmd_line = " ".join(topsapp_cmd)
    logger.info("Calling topsApp.py to prepesd step: {}".format(topsapp_cmd_line))
    check_call(topsapp_cmd_line, shell=True)

    # iterate over ESD coherence thresholds
    esd_coh_increment = 0.05
    esd_coh_min = 0.5
    topsapp_cmd = [
        "topsApp.py", "--steps", "--dostep=esd",
    ]
    topsapp_cmd_line = " ".join(topsapp_cmd)
    while True:
        logger.info("Calling topsApp.py on esd step with ESD coherence threshold: {}".format(esd_coh_th))
        try:
            check_call(topsapp_cmd_line, shell=True)
            break
        except CalledProcessError:
            logger.info("ESD filtering failed with ESD coherence threshold: {}".format(esd_coh_th))
            esd_coh_th = round(esd_coh_th-esd_coh_increment, 2)
            if esd_coh_th < esd_coh_min:
                logger.info("Disabling ESD filtering.")
                do_esd = False
                create_input_xml(os.path.join(BASE_PATH, 'topsApp_standard_product.xml.tmpl'), xml_file,
                                 str(master_safe_dirs), str(slave_safe_dirs), 
                                 ctx['master_orbit_file'], ctx['slave_orbit_file'],
                                 master_pol, slave_pol, preprocess_dem_file, geocode_dem_file,
                                 "1, 2, 3" if ctx['stitch_subswaths_xt'] else ctx['swathnum'],
                                 ctx['azimuth_looks'], ctx['range_looks'], ctx['filter_strength'],
                                 "{} {} {} {}".format(*bbox), "True", do_esd,
                                 esd_coh_th)
                break
            logger.info("Stepping down ESD coherence threshold to: {}".format(esd_coh_th))
            logger.info("Creating topsApp.xml with ESD coherence threshold: {}".format(esd_coh_th))
            create_input_xml(os.path.join(BASE_PATH, 'topsApp_standard_product.xml.tmpl'), xml_file,
                             str(master_safe_dirs), str(slave_safe_dirs), 
                             ctx['master_orbit_file'], ctx['slave_orbit_file'],
                             master_pol, slave_pol, preprocess_dem_file, geocode_dem_file,
                             "1, 2, 3" if ctx['stitch_subswaths_xt'] else ctx['swathnum'],
                             ctx['azimuth_looks'], ctx['range_looks'], ctx['filter_strength'],
                             "{} {} {} {}".format(*bbox), "True", do_esd,
                             esd_coh_th)

    # run topsApp from rangecoreg to geocode
    topsapp_cmd = [
        "topsApp.py", "--steps", "--start=rangecoreg", "--end=geocode",
    ]
    topsapp_cmd_line = " ".join(topsapp_cmd)
    logger.info("Calling topsApp.py to geocode step: {}".format(topsapp_cmd_line))
    check_call(topsapp_cmd_line, shell=True)

    #topsApp End Time
    topsApp_end_time=datetime.now() 
    logger.info("TopsApp End Time : {}".format(topsApp_end_time))

    topsApp_run_time=topsApp_end_time - topsApp_start_time
    logger.info("New TopsApp Run Time : {}".format(topsApp_run_time))

    swath_list = [1, 2, 3]
    met_files=[]
    ds_files=[]

    # get radian value for 5-cm wrap. As it is same for all swath, we will use swathnum = 1
    rt = parse('master/IW{}.xml'.format(1))
    wv = eval(rt.xpath('.//property[@name="radarwavelength"]/value/text()')[0])
    rad = 4 * np.pi * .05 / wv
    logger.info("Radian value for 5-cm wrap is: {}".format(rad))

    # create id and product directory

    output = get_tops_metadata('fine_interferogram')
    sensing_start= output['sensingStart']
    sensing_stop = output['sensingStop']
    logger.info("sensing_start : %s" %sensing_start)
    logger.info("sensing_stop : %s" %sensing_stop)

    acq_center_time = get_center_time(sensing_start, sensing_stop)


    ifg_hash = ctx["ifg_hash"]
    direction = ctx["direction"]
    #west_lat = ctx["west_lat"]
    platform = ctx["platform"]
    orbit_type = ctx["orbit_type"]
    track= ctx["track_number"]
    slave_ifg_dt = ctx['slave_ifg_dt']
    master_ifg_dt = ctx['master_ifg_dt']


    lats = get_geocoded_lats("merged/filt_topophase.unw.geo.vrt")


    sat_direction = "D"
    west_lat= "{}_{}".format(convert_number(sorted(lats)[-2]), convert_number(min(lats)))

    if direction.lower() == 'asc':
        sat_direction = "A"
        west_lat= "{}_{}".format(convert_number(max(lats)), convert_number(sorted(lats)[1]))



   
    ifg_id = IFG_ID_SP_TMPL.format(sat_direction, "R", track, master_ifg_dt, slave_ifg_dt, acq_center_time, west_lat, ifg_hash, version.replace('.', '_'))
    ifg_id_merged = id.replace('S1-IFG', 'S1-IFG-MERGED') 
    id = ifg_id

    logger.info("id : %s" %id)

    prod_dir = id
    prod_dir_merged = ifg_id_merged
    os.makedirs(prod_dir, 0o755)
    os.makedirs(prod_dir_merged, 0o755)

    # make metadata geocube
    os.chdir("merged")
    mgc_cmd = [
        "{}/makeGeocube.py".format(BASE_PATH), "-m", "../master",
        "-s", "../slave", "-o", "metadata.h5"
    ]
    mgc_cmd_line = " ".join(mgc_cmd)
    logger.info("Calling makeGeocube.py: {}".format(mgc_cmd_line))
    check_call(mgc_cmd_line, shell=True)

    # create standard product packaging
    #std_prod_file = "{}.hdf5".format(id)
    std_prod_file = "{}.nc".format(id)

    with open(os.path.join(BASE_PATH, "tops_groups.json")) as f:
        std_cfg = json.load(f)
    std_cfg['filename'] = std_prod_file
    with open('tops_groups.json', 'w') as f:
        json.dump(std_cfg, f, indent=2, sort_keys=True)
    std_cmd = [
        "{}/standard_product_packaging.py".format(BASE_PATH)
    ]
    std_cmd_line = " ".join(std_cmd)
    logger.info("Calling standard_product_packaging.py: {}".format(std_cmd_line))
    check_call(std_cmd_line, shell=True)

    # chdir back up to work directory
    os.chdir(cwd)

    # move standard product to product directory
    shutil.move(os.path.join('merged', std_prod_file), prod_dir)

    # generate GDAL (ENVI) headers and move to product directory
    raster_prods = (
        'merged/topophase.cor',
        'merged/topophase.flat',
        'merged/filt_topophase.flat',
        'merged/filt_topophase.unw',
        'merged/filt_topophase.unw.conncomp',
        'merged/phsig.cor',
        'merged/los.rdr',
        'merged/dem.crop',
    )
    for i in raster_prods:
        # radar-coded products
        call_noerr("isce2gis.py envi -i {}".format(i))
        gdal_xml = "{}.xml".format(i)
        gdal_hdr = "{}.hdr".format(i)
        gdal_vrt = "{}.vrt".format(i)

        # geo-coded products
        j = "{}.geo".format(i)
        if not os.path.exists(j): continue
        call_noerr("isce2gis.py envi -i {}".format(j))
        gdal_xml = "{}.xml".format(j)
        gdal_hdr = "{}.hdr".format(j)
        gdal_vrt = "{}.vrt".format(j)

    # save other files to product directory
    shutil.copyfile("_context.json", os.path.join(prod_dir,"{}.context.json".format(id)))
    shutil.copyfile("_context.json", os.path.join(prod_dir_merged,"{}.context.json".format(ifg_id_merged)))

    fine_int_xmls = []
    for swathnum in swath_list:
        fine_int_xmls.append("fine_interferogram/IW{}.xml".format(swathnum))
    
    # get water mask configuration
    wbd_url = uu.wbd_url
    wbd_user = uu.wbd_u
    wbd_pass = uu.wbd_p

    # get DEM bbox
    dem_S, dem_N, dem_W, dem_E = bbox
    dem_S = int(math.floor(dem_S))
    dem_N = int(math.ceil(dem_N))
    dem_W = int(math.floor(dem_W))
    dem_E = int(math.ceil(dem_E))

    # get water mask
    fp = open('wbdStitcher.xml','w')
    fp.write('<stitcher>\n')
    fp.write('    <component name="wbdstitcher">\n')
    fp.write('        <component name="wbd stitcher">\n')
    fp.write('            <property name="url">\n')
    fp.write('                <value>https://urlToRepository</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="action">\n')
    fp.write('                <value>stitch</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="directory">\n')
    fp.write('                <value>outputdir</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="bbox">\n')
    fp.write('                <value>[33,36,-119,-117]</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="keepWbds">\n')
    fp.write('                <value>False</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="noFilling">\n')
    fp.write('                <value>False</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="nodata">\n')
    fp.write('                <value>-1</value>\n')
    fp.write('            </property>\n')
    fp.write('        </component>\n')
    fp.write('    </component>\n')
    fp.write('</stitcher>')
    fp.close()
    wbd_file = "wbdmask.wbd"
    wbd_cmd = [
        "{}/applications/wbdStitcher.py".format(os.environ['ISCE_HOME']), "wbdStitcher.xml",
        "wbdstitcher.wbdstitcher.bbox=[{},{},{},{}]".format(dem_S, dem_N, dem_W, dem_E),
        "wbdstitcher.wbdstitcher.outputfile={}".format(wbd_file),
        "wbdstitcher.wbdstitcher.url={}".format(wbd_url)
    ]
    wbd_cmd_line = " ".join(wbd_cmd)
    logger.info("Calling wbdStitcher.py: {}".format(wbd_cmd_line))
    check_call(wbd_cmd_line, shell=True)

    # get product image and size info
    vrt_prod = get_image("merged/filt_topophase.unw.geo.xml")
    vrt_prod_size = get_size(vrt_prod)
    flat_vrt_prod = get_image("merged/filt_topophase.flat.geo.xml")
    flat_vrt_prod_size = get_size(flat_vrt_prod)

    # get water mask image and size info
    wbd_xml = "{}.xml".format(wbd_file)
    wmask = get_image(wbd_xml)
    wmask_size = get_size(wmask)

    # determine downsample ratio and dowsample water mask
    lon_rat = 1./(vrt_prod_size['lon']['delta']/wmask_size['lon']['delta'])*100
    lat_rat = 1./(vrt_prod_size['lat']['delta']/wmask_size['lat']['delta'])*100
    logger.info("lon_rat/lat_rat: {} {}".format(lon_rat, lat_rat))
    wbd_ds_file = "wbdmask_ds.wbd"
    wbd_ds_vrt = "wbdmask_ds.vrt"
    check_call("gdal_translate -of ENVI -outsize {}% {}% {} {}".format(lon_rat, lat_rat, wbd_file, wbd_ds_file), shell=True)
    check_call("gdal_translate -of VRT {} {}".format(wbd_ds_file, wbd_ds_vrt), shell=True)

    # update xml file for downsampled water mask
    wbd_ds_json = "{}.json".format(wbd_ds_file)
    check_call("gdalinfo -json {} > {}".format(wbd_ds_file, wbd_ds_json), shell=True)
    with open(wbd_ds_json) as f:
        info = json.load(f)
    with open(wbd_xml) as f:
        doc = parse(f)
    wbd_ds_xml = "{}.xml".format(wbd_ds_file)
    doc.xpath('.//component[@name="coordinate1"]/property[@name="delta"]/value')[0].text = str(info['geoTransform'][1])
    doc.xpath('.//component[@name="coordinate1"]/property[@name="size"]/value')[0].text = str(info['size'][0])
    doc.xpath('.//component[@name="coordinate2"]/property[@name="delta"]/value')[0].text = str(info['geoTransform'][5])
    doc.xpath('.//component[@name="coordinate2"]/property[@name="size"]/value')[0].text = str(info['size'][1])
    doc.xpath('.//property[@name="width"]/value')[0].text = str(info['size'][0])
    doc.xpath('.//property[@name="length"]/value')[0].text = str(info['size'][1])
    doc.xpath('.//property[@name="metadata_location"]/value')[0].text = wbd_ds_xml
    doc.xpath('.//property[@name="file_name"]/value')[0].text = wbd_ds_file
    for rm in doc.xpath('.//property[@name="extra_file_name"]'): rm.getparent().remove(rm)
    doc.write(wbd_ds_xml)

    # get downsampled water mask image and size info
    wmask_ds = get_image(wbd_ds_xml)
    wmask_ds_size = get_size(wmask_ds)

    logger.info("vrt_prod.filename: {}".format(vrt_prod.filename))
    logger.info("vrt_prod.bands: {}".format(vrt_prod.bands))
    logger.info("vrt_prod size: {}".format(vrt_prod_size))
    logger.info("wmask.filename: {}".format(wmask.filename))
    logger.info("wmask.bands: {}".format(wmask.bands))
    logger.info("wmask size: {}".format(wmask_size))
    logger.info("wmask_ds.filename: {}".format(wmask_ds.filename))
    logger.info("wmask_ds.bands: {}".format(wmask_ds.bands))
    logger.info("wmask_ds size: {}".format(wmask_ds_size))

    # crop the downsampled water mask
    wbd_cropped_file = "wbdmask_cropped.wbd"
    wmask_cropped = crop_mask(vrt_prod, wmask_ds, wbd_cropped_file)
    logger.info("wmask_cropped shape: {}".format(wmask_cropped.shape))

    # read in wrapped interferogram
    flat_vrt_prod_shape = (flat_vrt_prod_size['lat']['size'], flat_vrt_prod_size['lon']['size'])
    flat_vrt_prod_im = np.memmap(flat_vrt_prod.filename,
                            dtype=flat_vrt_prod.toNumpyDataType(),
                            mode='c', shape=(flat_vrt_prod_size['lat']['size'], flat_vrt_prod_size['lon']['size']))
    phase = np.angle(flat_vrt_prod_im)
    phase[phase == 0] = -10
    phase[wmask_cropped == -1] = -10

    # mask out water from the product data
    vrt_prod_shape = (vrt_prod_size['lat']['size'], vrt_prod.bands, vrt_prod_size['lon']['size'])
    vrt_prod_im = np.memmap(vrt_prod.filename,
                            dtype=vrt_prod.toNumpyDataType(),
                            mode='c', shape=vrt_prod_shape)
    im1 = vrt_prod_im[:,:,:]
    for i in range(vrt_prod.bands):
        im1_tmp = im1[:,i,:]
        im1_tmp[wmask_cropped == -1] = 0

    # read in connected component mask
    cc_vrt = "merged/filt_topophase.unw.conncomp.geo.vrt"
    cc = gdal.Open(cc_vrt)
    cc_data = cc.ReadAsArray()
    cc = None
    logger.info("cc_data: {}".format(cc_data))
    logger.info("cc_data shape: {}".format(cc_data.shape))
    for i in range(vrt_prod.bands):
        im1_tmp = im1[:,i,:]
        im1_tmp[cc_data == 0] = 0
    phase[cc_data == 0] = -10

    # overwrite displacement with phase
    im1[:,1,:] = phase

    # create masked product image
    masked_filt = "filt_topophase.masked.unw.geo"
    masked_filt_xml = "filt_topophase.masked.unw.geo.xml"
    tim = np.memmap(masked_filt, dtype=vrt_prod.toNumpyDataType(), mode='w+', shape=vrt_prod_shape)
    tim[:,:,:] = im1
    im  = Image()
    with open("merged/filt_topophase.unw.geo.xml") as f:
        doc = parse(f)
    doc.xpath('.//property[@name="file_name"]/value')[0].text = masked_filt
    for rm in doc.xpath('.//property[@name="extra_file_name"]'): rm.getparent().remove(rm)
    doc.write(masked_filt_xml)
    im.load(masked_filt_xml)
    latstart = vrt_prod_size['lat']['val']
    lonstart = vrt_prod_size['lon']['val']
    latsize = vrt_prod_size['lat']['size']
    lonsize = vrt_prod_size['lon']['size']
    latdelta = vrt_prod_size['lat']['delta']
    londelta = vrt_prod_size['lon']['delta']
    im.coord2.coordStart = latstart
    im.coord2.coordSize = latsize
    im.coord2.coordDelta = latdelta
    im.coord2.coordEnd = latstart + latsize*latdelta
    im.coord1.coordStart = lonstart
    im.coord1.coordSize = lonsize
    im.coord1.coordDelta = londelta
    im.coord1.coordEnd = lonstart + lonsize*londelta
    im.filename = masked_filt
    im.renderHdr()

    # mask out nodata values
    vrt_prod_file = "filt_topophase.masked.unw.geo.vrt"
    vrt_prod_file_amp = "filt_topophase.masked_nodata.unw.amp.geo.vrt"
    vrt_prod_file_dis = "filt_topophase.masked_nodata.unw.dis.geo.vrt"
    check_call("gdal_translate -of VRT -b 1 -a_nodata 0 {} {}".format(vrt_prod_file, vrt_prod_file_amp), shell=True)
    check_call("gdal_translate -of VRT -b 2 -a_nodata -10 {} {}".format(vrt_prod_file, vrt_prod_file_dis), shell=True)

    # get band statistics
    amp_data = gdal.Open(vrt_prod_file_amp, gdal.GA_ReadOnly)
    band_stats_amp = amp_data.GetRasterBand(1).GetStatistics(0, 1)
    dis_data = gdal.Open(vrt_prod_file_dis, gdal.GA_ReadOnly)
    band_stats_dis = dis_data.GetRasterBand(1).GetStatistics(0, 1)
    logger.info("amplitude band stats: {}".format(band_stats_amp))
    logger.info("displacment band stats: {}".format(band_stats_dis))

    # create interferogram tile layer
    tiles_dir = "{}/tiles".format(prod_dir)
    tiler_cmd_path = os.path.abspath(os.path.join(BASE_PATH, '..', '..', 'map_tiler'))
    dis_layer = "interferogram"
    tiler_cmd_tmpl = "{}/create_tiles.py {} {}/{} -b 1 -m hsv --clim_min {} --clim_max {} --nodata 0"
    check_call(tiler_cmd_tmpl.format(tiler_cmd_path, vrt_prod_file_dis, tiles_dir, dis_layer, band_stats_dis[0], band_stats_dis[1]), shell=True)

    # create amplitude tile layer
    amp_layer = "amplitude"
    tiler_cmd_tmpl = "{}/create_tiles.py {} {}/{} -b 1 -m gray --clim_min {} --clim_max {} --nodata 0"
    check_call(tiler_cmd_tmpl.format(tiler_cmd_path, vrt_prod_file_amp, tiles_dir, amp_layer, band_stats_amp[0], band_stats_amp[1]), shell=True)

    # create browse images
    tif_file_dis = "{}.tif".format(vrt_prod_file_dis)
    check_call("gdal_translate -of png -r average -tr 0.00416666667 0.00416666667 {} {}/{}.interferogram.browse_coarse.png".format(tif_file_dis, prod_dir, id), shell=True)
    check_call("gdal_translate -of png {} {}/{}.interferogram.browse_full.png".format(tif_file_dis, prod_dir, id), shell=True)
    tif_file_amp = "{}.tif".format(vrt_prod_file_amp)
    check_call("gdal_translate -of png -r average -tr 0.00416666667 0.00416666667 {} {}/{}.amplitude.browse_coarse.png".format(tif_file_amp, prod_dir, id), shell=True)
    check_call("gdal_translate -of png {} {}/{}.amplitude.browse_full.png".format(tif_file_amp, prod_dir, id), shell=True)
    for i in glob("{}/{}.*.browse*.aux.xml".format(prod_dir, id)): os.unlink(i)

    # extract metadata from master
    met_file = os.path.join(prod_dir, "{}.met.json".format(id))
    extract_cmd_path = os.path.abspath(os.path.join(BASE_PATH, '..', 
                                                    '..', 'frameMetadata',
                                                    'sentinel'))
    extract_cmd_tmpl = "{}/extractMetadata_standard_product.sh -i {}/annotation/s1?-iw?-slc-{}-*.xml -o {}"
    check_call(extract_cmd_tmpl.format(extract_cmd_path, master_safe_dirs[0],
                                       master_pol, met_file),shell=True)
    
    # update met JSON
    if 'RESORB' in ctx['master_orbit_file'] or 'RESORB' in ctx['slave_orbit_file']:
        orbit_type = 'resorb'
    else: orbit_type = 'poeorb'
    scene_count = min(len(master_safe_dirs), len(slave_safe_dirs))
    master_mission = MISSION_RE.search(master_safe_dirs[0]).group(1)
    slave_mission = MISSION_RE.search(slave_safe_dirs[0]).group(1)
    unw_vrt = "filt_topophase.unw.geo.vrt"
    unw_xml = "filt_topophase.unw.geo.xml"
    update_met_cmd = '{}/update_met_json_standard_product.py {} {} "{}" {} {} {} "{}" {}/{} {}/{} {} {} {} {}'
    check_call(update_met_cmd.format(BASE_PATH, orbit_type, scene_count,
                                     ctx['swathnum'], master_mission,
                                     slave_mission, 'PICKLE',
                                     fine_int_xmls,
                                     'merged', unw_vrt,
                                     'merged', unw_xml,
                                     met_file, sensing_start,
                                     sensing_stop, std_prod_file), shell=True)

    # add master/slave ids and orbits to met JSON (per ASF request)
    master_ids = [i.replace(".zip", "") for i in ctx['master_zip_file']]
    slave_ids = [i.replace(".zip", "") for i in ctx['slave_zip_file']]
    master_rt = parse("master/IW1.xml")
    master_orbit_number = eval(master_rt.xpath('.//property[@name="orbitnumber"]/value/text()')[0])
    slave_rt = parse("slave/IW1.xml")
    slave_orbit_number = eval(slave_rt.xpath('.//property[@name="orbitnumber"]/value/text()')[0])
    with open(met_file) as f: md = json.load(f)
    md['master_scenes'] = master_ids
    md['slave_scenes'] = slave_ids
    md['orbitNumber'] = [master_orbit_number, slave_orbit_number]
    #if ctx.get('stitch_subswaths_xt', False): md['swath'] = [1, 2, 3]
    md['esd_threshold'] = esd_coh_th if do_esd else -1.  # add ESD coherence threshold

    # add range_looks and azimuth_looks to metadata for stitching purposes
    md['azimuth_looks'] = int(ctx['azimuth_looks'])
    md['range_looks'] = int(ctx['range_looks'])

    # add filter strength
    md['filter_strength'] = float(ctx['filter_strength'])
    md['union_geojson'] = ctx['union_geojson']
    # add dem_type
    md['dem_type'] = dem_type
    md['sensingStart'] = sensing_start
    md['sensingStop'] = sensing_stop
    md['tags'] = ['standard_product']

    #update met files key to have python style naming
    md = update_met(md)

    # write met json
    logger.info("creating met file : %s" %met_file)
    with open(met_file, 'w') as f: json.dump(md, f, indent=2)
    
    # generate dataset JSON
    ds_file = os.path.join(prod_dir, "{}.dataset.json".format(id))
    logger.info("creating dataset file : %s" %ds_file)
    create_dataset_json(id, version, met_file, ds_file)


    #copy files to merged directory
    met_file_merged = os.path.join(prod_dir_merged, "{}.met.json".format(ifg_id_merged))
    ds_file_merged = os.path.join(prod_dir_merged, "{}.dataset.json".format(ifg_id_merged))
    shutil.copy(ds_file, ds_file_merged)
    shutil.copy(met_file, met_file_merged)
    shutil.copytree("merged", os.path.join(prod_dir_merged, "merged"))
    #shutil.copytree(tiles_dir, os.path.join(prod_dir_merged, "tiles"))
    
    #logger.info( json.dump(md, f, indent=2))

    # clean out SAFE directories, DEM files and water masks
    for i in chain(master_safe_dirs, slave_safe_dirs): shutil.rmtree(i)
    for i in glob("dem*"): os.unlink(i)
    for i in glob("wbdmask*"): os.unlink(i)

    #topsApp End Time
    complete_end_time=datetime.now()
    logger.info("TopsApp End Time : {}".format(complete_end_time))

    complete_run_time=complete_end_time - complete_start_time
    logger.info("New TopsApp Run Time : {}".format(complete_run_time))


if __name__ == '__main__':
    try: status = main()
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)

