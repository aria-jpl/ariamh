#!/usr/bin/env python3 
import os, sys, re, requests, json, shutil, traceback, logging, hashlib, math
from itertools import chain
from zipfile import ZipFile
from subprocess import check_call, CalledProcessError
from glob import glob
from lxml.etree import parse
import numpy as np
from datetime import datetime
from osgeo import ogr

from utils.UrlUtils_standard_product import UrlUtils
from check_interferogram import check_int
from create_input_xml_standard_product import create_input_xml


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_ifg')


BASE_PATH = os.path.dirname(__file__)


KILAUEA_DEM_XML = "https://aria-alt-dav.jpl.nasa.gov/repository/products/kilauea/dem_kilauea.dem.xml"
KILAUEA_DEM = "https://aria-alt-dav.jpl.nasa.gov/repository/products/kilauea/dem_kilauea.dem"


MISSION_RE = re.compile(r'^(S1\w)_')
POL_RE = re.compile(r'^S1\w_IW_SLC._1S(\w{2})_')


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
    if isinstance(md['sensingStart'], str):
        ds['starttime'] = md['sensingStart']
    else:
        md['sensingStart'].sort()
        ds['starttime'] = md['sensingStart'][0]

    if isinstance(md['sensingStop'], str):
        ds['endtime'] = md['sensingStop']
    else:
        md['sensingStop'].sort()
        ds['endtime'] = md['sensingStop'][-1]

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
        'swath': [1, 2, 3],
        'trackNumber': [],
        'archive_filename': id,
        'dataset_type': 'slc',
        'tile_layers': [ 'amplitude', 'displacement' ],
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
    set_params = ('master_scenes', 'esd_threshold', 'frameID', 'swath', 'parallelBaseline',
                  'doppler', 'version', 'slave_scenes', 'orbit_type', 'spacecraftName',
                  'orbitNumber', 'perpendicularBaseline', 'orbitRepeat', 'polarization', 
                  'sensor', 'lookDirection', 'platform', 'startingRange',
                  'beamMode', 'direction', 'prf', 'azimuth_looks')
    single_params = ('temporal_span', 'trackNumber', 'dem_type')
    list_params = ('platform', 'swath', 'perpendicularBaseline', 'parallelBaseline', 'range_looks','filter_strength')
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


def move_dem_separate_dir (dir_name):
    create_dir(dir_name)

    move_cmd=["mv", "demLat*", dir_name]
    move_cmd_line=" ".join(move_cmd)
    logger.info("Calling {}".format(move_cmd_line))
    call_noerr(move_cmd_line)

    move_cmd=["mv", "stitched.*", dir_name]
    move_cmd_line=" ".join(move_cmd)
    logger.info("Calling {}".format(move_cmd_line))
    call_noerr(move_cmd_line)

    move_cmd=["mv", "*DEM.vrt", dir_name]
    move_cmd_line=" ".join(move_cmd)
    logger.info("Calling {}".format(move_cmd_line))
    call_noerr(move_cmd_line)

def create_dir(dir_name):
    if os.path.isdir(dir_name):
        rmdir_cmd=["rm", "-rf", dir_name]
        rmdir_cmd_line=" ".join(rmdir_cmd)
        logger.info("Calling {}".format(rmdir_cmd_line))
        call_noerr(rmdir_cmd_line)

    mkdir_cmd=["mkdir", dir_name]
    mkdir_cmd_line=" ".join(mkdir_cmd)
    logger.info("Calling {}".format(mkdir_cmd_line))
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
        preprocess_dem_file = os.path.basename(KILAUEA_DEM)
    else:
        # get DEM bbox
        dem_S, dem_N, dem_W, dem_E = bbox
        dem_S = int(math.floor(dem_S))
        dem_N = int(math.ceil(dem_N))
        dem_W = int(math.floor(dem_W))
        dem_E = int(math.ceil(dem_E))
        

        if dem_type.startswith("SRTM"):
            if dem_type.startswith("SRTM3"):
                dem_url = srtm3_dem_url
  
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
            if dem_type == "NED1": dem_url = ned1_dem_url
            elif dem_type.startswith("NED13"): dem_url = ned13_dem_url
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
    logger.info("Using Preprocess DEM file: {}".format(preprocess_dem_file))

    move_dem_separate_dir(preprocess_dem_dir)
    preprocess_dem_file = os.path.join(preprocess_dem_dir, preprocess_dem_file)

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
        print("preprocess_vrt_file : %s"%preprocess_vrt_file)
    else: raise RuntimeError("Unknown dem type %s." % dem_type)

    if not os.path.isfile(preprocess_vrt_file):
        print("%s does not exists. Exiting")
    
    geocode_dem_dir = os.path.join(preprocess_dem_dir, "Coarse_preprocess_dem")
    create_dir(geocode_dem_dir)

    dem_cmd = [
        "{}/applications/downsampleDEM.py".format(os.environ['ISCE_HOME']), "-i",
        "{}".format(preprocess_vrt_file), "-r", "90"
    ]
    dem_cmd_line = " ".join(dem_cmd)
    logger.info("Calling downsampleDEM.py: {}".format(dem_cmd_line))
    check_call(dem_cmd_line, shell=True)
    geocode_dem_file = ""

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

    # create product directory
    prod_dir = id
    os.makedirs(prod_dir, 0o755)

    # create merged directory in product
    prod_merged_dir = os.path.join(prod_dir, 'merged')
    os.makedirs(prod_merged_dir, 0o755)

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
    std_prod_file = "{}.hdf5".format(id)
    std_cmd = [
        "{}/standard_product_packaging.py".format(BASE_PATH),
        std_prod_file
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
        #call_noerr("gdal_translate {} {}.tif".format(i, i))
        gdal_xml = "{}.xml".format(i)
        gdal_hdr = "{}.hdr".format(i)
        #gdal_tif = "{}.tif".format(i)
        gdal_vrt = "{}.vrt".format(i)
        if os.path.exists(i): shutil.move(i, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(i))
        if os.path.exists(gdal_xml): shutil.move(gdal_xml, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_xml))
        if os.path.exists(gdal_hdr): shutil.move(gdal_hdr, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_hdr))
        #if os.path.exists(gdal_tif): shutil.move(gdal_tif, prod_merged_dir)
        #else: logger.warn("{} wasn't generated.".format(gdal_tif))
        if os.path.exists(gdal_vrt): shutil.move(gdal_vrt, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_vrt))

        # geo-coded products
        j = "{}.geo".format(i)
        if not os.path.exists(j): continue
        call_noerr("isce2gis.py envi -i {}".format(j))
        #call_noerr("gdal_translate {} {}.tif".format(j, j))
        gdal_xml = "{}.xml".format(j)
        gdal_hdr = "{}.hdr".format(j)
        #gdal_tif = "{}.tif".format(j)
        gdal_vrt = "{}.vrt".format(j)
        if os.path.exists(j): shutil.move(j, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(j))
        if os.path.exists(gdal_xml): shutil.move(gdal_xml, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_xml))
        if os.path.exists(gdal_hdr): shutil.move(gdal_hdr, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_hdr))
        #if os.path.exists(gdal_tif): shutil.move(gdal_tif, prod_merged_dir)
        #else: logger.warn("{} wasn't generated.".format(gdal_tif))
        if os.path.exists(gdal_vrt): shutil.move(gdal_vrt, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_vrt))

    # save other files to product directory
    shutil.copyfile("_context.json", os.path.join(prod_dir,"{}.context.json".format(id)))
    shutil.copyfile("topsApp.xml", os.path.join(prod_dir, "topsApp.xml"))
    if os.path.exists('topsProc.xml'):
        shutil.copyfile("topsProc.xml", os.path.join(prod_dir, "topsProc.xml"))
    if os.path.exists('isce.log'):
        shutil.copyfile("isce.log", os.path.join(prod_dir, "isce.log"))

    # move PICKLE to product directory
    shutil.move('PICKLE', prod_dir)

    fine_int_xmls = []
    for swathnum in swath_list:
       # ctx['swathnum'] = swathnum
        fine_int_xml = "fine_interferogram_IW{}.xml".format(swathnum)
        master_xml="master_IW{}.xml".format(swathnum)
        slave_xml = "slave_IW{}.xml".format(swathnum)

        fine_int_xmls.append(os.path.join(prod_dir, fine_int_xml))

        logger.info("\n\nPROCESSING SWATH : {}".format(swathnum))

        shutil.copyfile("fine_interferogram/IW{}.xml".format(swathnum),
                    os.path.join(prod_dir, fine_int_xml))
        shutil.copyfile("master/IW{}.xml".format(swathnum),
                    os.path.join(prod_dir, master_xml))
        shutil.copyfile("slave/IW{}.xml".format(swathnum),
                    os.path.join(prod_dir, slave_xml))

    
    # create browse images
    os.chdir(prod_merged_dir)
    mdx_app_path = "{}/applications/mdx.py".format(os.environ['ISCE_HOME'])
    mdx_path = "{}/bin/mdx".format(os.environ['ISCE_HOME'])
    from utils.createImage import createImage
    unw_file = "filt_topophase.unw.geo"
    #unwrapped image at different rates
    createImage("{} -P {}".format(mdx_app_path, unw_file),unw_file)
    createImage("{} -P {} -wrap {}".format(mdx_app_path, unw_file, rad),unw_file + "_5cm")
    createImage("{} -P {} -wrap 20".format(mdx_app_path, unw_file),unw_file + "_20rad")
    #amplitude image
    unw_xml = "filt_topophase.unw.geo.xml"
    rt = parse(unw_xml)
    size = eval(rt.xpath('.//component[@name="coordinate1"]/property[@name="size"]/value/text()')[0])
    rtlr = size * 4
    logger.info("rtlr value for amplitude browse is: {}".format(rtlr))
    createImage("{} -P {} -s {} -amp -r4 -rtlr {} -CW".format(mdx_path, unw_file, size, rtlr),'amplitude.geo')
    #coherence image
    top_file = "topophase.cor.geo"
    createImage("{} -P {}".format(mdx_app_path, top_file),top_file)
    #should be the same size as unw but just in case
    top_xml = "topophase.cor.geo.xml"
    rt = parse(top_xml)
    size = eval(rt.xpath('.//component[@name="coordinate1"]/property[@name="size"]/value/text()')[0])
    rhdr = size * 4
    createImage("{} -P {} -s {} -r4 -rhdr {} -cmap cmy -wrap 1.2".format(mdx_path, top_file,size,rhdr),"topophase_ph_only.cor.geo")


    '''
    # unw browse
    unw_file = "filt_topophase.unw.geo"
    unw_browse_img = unw_file + ".browse.png"
    unw_browse_img_small = unw_file + ".browse_small.png"
    call_noerr("{} -P {}".format(mdx_app_path, unw_file))
    call_noerr("convert out.ppm -transparent black -trim {}".format(unw_browse_img))
    call_noerr("convert -resize 250x250 {} {}".format(unw_browse_img, unw_browse_img_small))
    if os.path.exists('out.ppm'): os.unlink('out.ppm')

    # unw 5cm browse
    unw_5cm_browse_img = "unw.geo_5cm.browse.png"
    unw_5cm_browse_img_small = "unw.geo_5cm.browse_small.png"
    call_noerr("{} -P {} -wrap {}".format(mdx_app_path, unw_file, rad))
    call_noerr("convert out.ppm -transparent black -trim {}".format(unw_5cm_browse_img))
    call_noerr("convert -resize 250x250 {} {}".format(unw_5cm_browse_img, unw_5cm_browse_img_small))
    if os.path.exists('out.ppm'): os.unlink('out.ppm')

    # unw 20rad browse
    unw_20rad_browse_img = "unw.geo_20rad.browse.png"
    unw_20rad_browse_img_small = "unw.geo_20rad.browse_small.png"
    call_noerr("{} -P {} -wrap 20".format(mdx_app_path, unw_file))
    call_noerr("convert out.ppm -transparent black -trim {}".format(unw_20rad_browse_img))
    call_noerr("convert -resize 250x250 {} {}".format(unw_20rad_browse_img, unw_20rad_browse_img_small))
    if os.path.exists('out.ppm'): os.unlink('out.ppm')

    # amplitude browse
    unw_xml = "filt_topophase.unw.geo.xml"
    amplitude_browse_img = "amplitude.geo.browse.png"
    amplitude_browse_img_small = "amplitude.geo.browse_small.png"
    rt = parse(unw_xml)
    size = eval(rt.xpath('.//component[@name="coordinate1"]/property[@name="size"]/value/text()')[0])
    rtlr = size * 4
    logger.info("rtlr value for amplitude browse is: {}".format(rtlr))
    call_noerr("{} -P {} -s {} -amp -r4 -rtlr {} -CW".format(mdx_path, unw_file, size, rtlr))
    call_noerr("convert out.ppm -transparent black -trim {}".format(amplitude_browse_img))
    call_noerr("convert -resize 250x250 {} {}".format(amplitude_browse_img, amplitude_browse_img_small))
    if os.path.exists('out.ppm'): os.unlink('out.ppm')

    # topophase browse
    top_file = "topophase.cor.geo"
    top_browse_img = "top.geo.browse.png"
    top_browse_img_small = "top.geo.browse_small.png"
    call_noerr("{} -P {}".format(mdx_app_path, top_file))
    call_noerr("convert out.ppm -transparent black -trim {}".format(top_browse_img))
    call_noerr("convert -resize 250x250 {} {}".format(top_browse_img, top_browse_img_small))
    if os.path.exists('out.ppm'): os.unlink('out.ppm')
    '''
    # create unw KMZ
    unw_kml = "unw.geo.kml"
    unw_kmz = "{}.kmz".format(id)
    call_noerr("{} {} -kml {}".format(mdx_app_path, unw_file, unw_kml))
    call_noerr("{}/create_kmz.py {} {}.png {}".format(BASE_PATH, unw_kml, unw_file, unw_kmz))

    # move all browse images to root of product directory
    call_noerr("mv -f *.png *.kmz ..")

    # remove kml
    call_noerr("rm -f *.kml")

    # chdir back up to work directory
    os.chdir(cwd)

    # create displacement tile layer
    tiles_dir = "{}/tiles".format(prod_dir)
    vrt_prod_file = "{}/merged/filt_topophase.unw.geo.vrt".format(prod_dir)
    tiler_cmd_path = os.path.abspath(os.path.join(BASE_PATH, '..', '..', 'map_tiler'))
    dis_layer = "displacement"
    tiler_cmd_tmpl = "{}/create_tiles.py {} {}/{} -b 2 -m prism --nodata 0"
    check_call(tiler_cmd_tmpl.format(tiler_cmd_path, vrt_prod_file, tiles_dir, dis_layer), shell=True)

    # create amplitude tile layer
    amp_layer = "amplitude"
    tiler_cmd_tmpl = "{}/create_tiles.py {} {}/{} -b 1 -m gray --clim_min 10 --clim_max_pct 80 --nodata 0"
    check_call(tiler_cmd_tmpl.format(tiler_cmd_path, vrt_prod_file, tiles_dir, amp_layer), shell=True)

    # create COG (cloud optimized geotiff) with no_data set
    cog_prod_file = "{}/merged/filt_topophase.unw.geo.tif".format(prod_dir)
    cog_cmd_tmpl = "gdal_translate {} tmp.tif -co TILED=YES -co COMPRESS=DEFLATE -a_nodata 0"
    check_call(cog_cmd_tmpl.format(vrt_prod_file), shell=True)
    check_call("gdaladdo -r average tmp.tif 2 4 8 16 32", shell=True)
    cog_cmd_tmpl = "gdal_translate tmp.tif {} -co TILED=YES -co COPY_SRC_OVERVIEWS=YES -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 --config GDAL_TIFF_OVR_BLOCKSIZE 512"
    check_call(cog_cmd_tmpl.format(cog_prod_file), shell=True)
    os.unlink("tmp.tif")


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
    #fine_int_xml = "fine_interferogram.xml"
    update_met_cmd = '{}/update_met_json_standard_product.py {} {} "{}" {} {} {}/{} "{}" {}/{} {}/{} {}'
    check_call(update_met_cmd.format(BASE_PATH, orbit_type, scene_count,
                                     ctx['swathnum'], master_mission,
                                     slave_mission, prod_dir, 'PICKLE',
                                     fine_int_xmls,
                                     prod_merged_dir, unw_vrt,
                                     prod_merged_dir, unw_xml,
                                     met_file), shell=True)

    # add master/slave ids and orbits to met JSON (per ASF request)
    master_ids = [i.replace(".zip", "") for i in ctx['master_zip_file']]
    slave_ids = [i.replace(".zip", "") for i in ctx['slave_zip_file']]
    master_rt = parse(os.path.join(prod_dir, master_xml))
    master_orbit_number = eval(master_rt.xpath('.//property[@name="orbitnumber"]/value/text()')[0])
    slave_rt = parse(os.path.join(prod_dir, slave_xml))
    slave_orbit_number = eval(slave_rt.xpath('.//property[@name="orbitnumber"]/value/text()')[0])
    with open(met_file) as f: md = json.load(f)
    md['master_scenes'] = master_ids
    md['slave_scenes'] = slave_ids
    md['orbitNumber'] = [master_orbit_number, slave_orbit_number]
    if ctx.get('stitch_subswaths_xt', False): md['swath'] = [1, 2, 3]
    md['esd_threshold'] = esd_coh_th if do_esd else -1.  # add ESD coherence threshold

    # add range_looks and azimuth_looks to metadata for stitching purposes
    md['azimuth_looks'] = int(ctx['azimuth_looks'])
    md['range_looks'] = int(ctx['range_looks'])

    # add filter strength
    md['filter_strength'] = float(ctx['filter_strength'])
    md['union_geojson'] = ctx['union_geojson']
    # add dem_type
    md['dem_type'] = dem_type

    # write met json
    print("creating met file : %s" %met_file)
    with open(met_file, 'w') as f: json.dump(md, f, indent=2)
    
    # generate dataset JSON
    ds_file = os.path.join(prod_dir, "{}.dataset.json".format(id))
    print("creating dataset file : %s" %ds_file)
    create_dataset_json(id, version, met_file, ds_file)
    
    #print( json.dump(md, f, indent=2))

    # move merged products to root of product directory
    #call_noerr("mv -f {}/* {}".format(prod_merged_dir, prod_dir))
    #shutil.rmtree(prod_merged_dir)

    # write PROV-ES JSON
    #${BASE_PATH}/create_prov_es-create_interferogram.sh $id $project $master_orbit_file $slave_orbit_file \
    #                                                        ${preprocess_dem_file}.xml $preprocess_dem_file $WORK_DIR \
    #                                                        ${id}/${id}.prov_es.json > create_prov_es.log 2>&1
    
    # clean out SAFE directories and DEM files
    #for i in chain(master_safe_dirs, slave_safe_dirs): shutil.rmtree(i)
    for i in glob("dem*"): os.unlink(i)

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

