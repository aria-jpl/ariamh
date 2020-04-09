#!/usr/bin/env python3 
from builtins import str
import os, sys, re, requests, json, shutil, traceback, logging, hashlib, math
from itertools import chain
from zipfile import ZipFile
from subprocess import check_call, CalledProcessError
from glob import glob
from lxml.etree import parse
import numpy as np
from datetime import datetime

from utils.UrlUtils import UrlUtils
from check_rsp import check_rsp
from create_input_xml import create_input_xml


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_rsp')


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
    return ds_vers['S1-SLCP']


def create_dataset_json(id, version, met_file, ds_file):
    """Write dataset json."""

    # get metadata
    with open(met_file) as f:
        md = json.load(f)

    # build dataset
    ds = {
        'creation_timestamp': "%sZ" % datetime.utcnow().isoformat(),
        'version': version,
        'label': id,
        'location': {
            'type': 'Polygon',
            'coordinates': [
                [   
                    [ md['bbox'][0][1], md['bbox'][0][0] ],
                    [ md['bbox'][1][1], md['bbox'][1][0] ],
                    [ md['bbox'][2][1], md['bbox'][2][0] ],
                    [ md['bbox'][3][1], md['bbox'][3][0] ],
                    [ md['bbox'][0][1], md['bbox'][0][0] ]
                ]
            ]
        }
    }

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


def rsp_exists(es_url, es_index, id):
    """Check slc_pair product exists in GRQ."""

    total, id = check_rsp(es_url, es_index, id)
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


def call_noerr(cmd):
    """Run command and warn if exit status is not 0."""

    try: check_call(cmd, shell=True)
    except Exception as e:
        logger.warn("Got exception running {}: {}".format(cmd, str(e)))
        logger.warn("Traceback: {}".format(traceback.format_exc()))


def main():
    """HySDS PGE wrapper for TopsInSAR slc_pair product generation."""

    # save cwd (working directory)
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

    # stitch all subswaths?
    ctx['stitch_subswaths_xt'] = False
    if ctx['swathnum'] is None:
        ctx['stitch_subswaths_xt'] = True
        ctx['swathnum'] = 1
        # use default azimuth and range looks for cross-swath stitching
        ctx['azimuth_looks'] = ctx.get("context", {}).get("azimuth_looks", 7)
        ctx['range_looks'] = ctx.get("context", {}).get("range_looks", 19)
    logger.info("Using azimuth_looks of %d and range_looks of %d" % (ctx['azimuth_looks'],ctx['range_looks']))

    ctx['filter_strength'] = ctx.get("context", {}).get("filter_strength", 0.5)
    logger.info("Using filter_strength of %f" % ctx['filter_strength'])

    # unzip SAFE dirs
    master_safe_dirs = []
    for i in ctx['master_zip_file']:
        master_safe_dir = i.replace(".zip", ".SAFE")
        if not os.path.exists(master_safe_dir):
            logger.info("Unzipping {}.".format(i))
            with ZipFile(i, 'r') as zf:
                zf.extractall()
            logger.info("Removing {}.".format(i))
            try: os.unlink(i)
            except: pass
        master_safe_dirs.append(master_safe_dir)
    slave_safe_dirs = []
    for i in ctx['slave_zip_file']:
        slave_safe_dir = i.replace(".zip", ".SAFE")
        if not os.path.exists(slave_safe_dir):
            logger.info("Unzipping {}.".format(i))
            with ZipFile(i, 'r') as zf:
                zf.extractall()
            logger.info("Removing {}.".format(i))
            try: os.unlink(i)
            except: pass
        slave_safe_dirs.append(slave_safe_dir)

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
    es_index = "{}_{}_s1-slcp".format(uu.grq_index_prefix, version)

    # check if slc_pair product already exists
    logger.info("GRQ url: {}".format(es_url))
    logger.info("GRQ index: {}".format(es_index))
    logger.info("Product ID for version {}: {}".format(version, id))
    if rsp_exists(es_url, es_index, id):
        logger.info("{} slc_pair product for {}".format(version, id_base) +
                     " was previously generated and exists in GRQ database.")

        # cleanup SAFE dirs
        for i in chain(master_safe_dirs, slave_safe_dirs):
            logger.info("Removing {}.".format(i))
            try: shutil.rmtree(i)
            except: pass
        return 0

    # get DEM configuration
    dem_type = ctx.get("context", {}).get("dem_type", "SRTM+v3")
    srtm_dem_url = uu.dem_url
    ned1_dem_url = uu.ned1_dem_url
    ned13_dem_url = uu.ned13_dem_url
    dem_user = uu.dem_u
    dem_pass = uu.dem_p
     
    # download project specific DEM
    if 'kilauea' in ctx['project']:
        s = requests.session()
        s.auth = (dem_user, dem_pass)
        download_file(KILAUEA_DEM_XML, session=s)
        download_file(KILAUEA_DEM, session=s)
        dem_file = os.path.basename(KILAUEA_DEM)
    else:
        # get DEM bbox
        dem_S, dem_N, dem_W, dem_E = bbox
        dem_S = int(math.floor(dem_S))
        dem_N = int(math.ceil(dem_N))
        dem_W = int(math.floor(dem_W))
        dem_E = int(math.ceil(dem_E))
        if dem_type == "SRTM+v3":
            dem_url = srtm_dem_url
            dem_cmd = [
                "{}/applications/dem.py".format(os.environ['ISCE_HOME']), "-a",
                "stitch", "-b", "{} {} {} {}".format(dem_S, dem_N, dem_W, dem_E),
                "-r", "-s", "1", "-f", "-x", "-c", "-n", dem_user, "-w", dem_pass,
                "-u", dem_url
            ]
            dem_cmd_line = " ".join(dem_cmd)
            logger.info("Calling dem.py: {}".format(dem_cmd_line))
            check_call(dem_cmd_line, shell=True)
            dem_file = glob("*.dem.wgs84")[0]
        else:
            if dem_type == "NED1": dem_url = ned1_dem_url
            elif dem_type.startswith("NED13"): dem_url = ned13_dem_url
            else: raise RuntimeError("Unknown dem type %s." % dem_type)
            if dem_type == "NED13-downsampled": downsample_option = "-d 33%"
            else: downsample_option = ""
            dem_cmd = [
                "{}/ned_dem.py".format(BASE_PATH), "-a",
                "stitch", "-b", "{} {} {} {}".format(dem_S, dem_N, dem_W, dem_E),
                downsample_option, "-u", dem_user, "-p", dem_pass, dem_url
            ]
            dem_cmd_line = " ".join(dem_cmd)
            logger.info("Calling ned_dem.py: {}".format(dem_cmd_line))
            check_call(dem_cmd_line, shell=True)
            dem_file = "stitched.dem"
    logger.info("Using DEM file: {}".format(dem_file))

    # fix file path in DEM xml
    fix_cmd = [
        "{}/applications/fixImageXml.py".format(os.environ['ISCE_HOME']),
        "-i", dem_file, "--full"
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
    create_input_xml(os.path.join(BASE_PATH, 'topsApp.xml.tmpl'), xml_file,
                     str(master_safe_dirs), str(slave_safe_dirs), 
                     ctx['master_orbit_file'], ctx['slave_orbit_file'],
                     master_pol, slave_pol, dem_file,
                     "1, 2, 3" if ctx['stitch_subswaths_xt'] else ctx['swathnum'],
                     ctx['azimuth_looks'], ctx['range_looks'], ctx['filter_strength'],
                     "{} {} {} {}".format(*bbox), "False", do_esd,
                     esd_coh_th)

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
                create_input_xml(os.path.join(BASE_PATH, 'topsApp.xml.tmpl'), xml_file,
                                 str(master_safe_dirs), str(slave_safe_dirs), 
                                 ctx['master_orbit_file'], ctx['slave_orbit_file'],
                                 master_pol, slave_pol, dem_file,
                                 "1, 2, 3" if ctx['stitch_subswaths_xt'] else ctx['swathnum'],
                                 ctx['azimuth_looks'], ctx['range_looks'], ctx['filter_strength'],
                                 "{} {} {} {}".format(*bbox), "True", do_esd,
                                 esd_coh_th)
                break
            logger.info("Stepping down ESD coherence threshold to: {}".format(esd_coh_th))
            logger.info("Creating topsApp.xml with ESD coherence threshold: {}".format(esd_coh_th))
            create_input_xml(os.path.join(BASE_PATH, 'topsApp.xml.tmpl'), xml_file,
                             str(master_safe_dirs), str(slave_safe_dirs), 
                             ctx['master_orbit_file'], ctx['slave_orbit_file'],
                             master_pol, slave_pol, dem_file,
                             "1, 2, 3" if ctx['stitch_subswaths_xt'] else ctx['swathnum'],
                             ctx['azimuth_looks'], ctx['range_looks'], ctx['filter_strength'],
                             "{} {} {} {}".format(*bbox), "True", do_esd,
                             esd_coh_th)

    # run topsApp from rangecoreg to geocode
    topsapp_cmd = [
        "topsApp.py", "--steps", "--start=rangecoreg", "--end=burstifg",
    ]
    topsapp_cmd_line = " ".join(topsapp_cmd)
    logger.info("Calling topsApp.py to geocode step: {}".format(topsapp_cmd_line))
    check_call(topsapp_cmd_line, shell=True)

    # create product directory
    prod_dir = id
    os.makedirs(prod_dir, 0o755)

    # save other files to product directory
    shutil.copyfile("_context.json", os.path.join(prod_dir,"{}.context.json".format(id)))
    shutil.copyfile("topsApp.xml", os.path.join(prod_dir, "topsApp.xml"))
    shutil.copyfile("fine_interferogram/IW{}.xml".format(ctx['swathnum']),
                    os.path.join(prod_dir, "fine_interferogram.xml"))
    shutil.copyfile("master/IW{}.xml".format(ctx['swathnum']),
                    os.path.join(prod_dir, "master.xml"))
    shutil.copyfile("slave/IW{}.xml".format(ctx['swathnum']),
                    os.path.join(prod_dir, "slave.xml"))
    if os.path.exists('topsProc.xml'):
        shutil.copyfile("topsProc.xml", os.path.join(prod_dir, "topsProc.xml"))
    if os.path.exists('isce.log'):
        shutil.copyfile("isce.log", os.path.join(prod_dir, "isce.log"))

    # extract metadata from master
    met_file = os.path.join(prod_dir, "{}.met.json".format(id))
    extract_cmd_path = os.path.abspath(os.path.join(BASE_PATH, '..', 
                                                    '..', 'frameMetadata',
                                                    'sentinel'))
    extract_cmd_tmpl = "{}/extractMetadata_s1.sh -i {}/annotation/s1?-iw{}-slc-{}-*.xml -o {}"
    check_call(extract_cmd_tmpl.format(extract_cmd_path, master_safe_dirs[0],
                                       ctx['swathnum'], master_pol, met_file),shell=True)
    
    # update met JSON
    if 'RESORB' in ctx['master_orbit_file'] or 'RESORB' in ctx['slave_orbit_file']:
        orbit_type = 'resorb'
    else: orbit_type = 'poeorb'
    scene_count = min(len(master_safe_dirs), len(slave_safe_dirs))
    master_mission = MISSION_RE.search(master_safe_dirs[0]).group(1)
    slave_mission = MISSION_RE.search(slave_safe_dirs[0]).group(1)
    fine_int_xml = "fine_interferogram.xml"
    update_met_cmd = "{}/update_met_json_rsp.py {} {} {} {} {} {} {}/{} {} {}"
    check_call(update_met_cmd.format(BASE_PATH, orbit_type, scene_count,
                                     ctx['swathnum'], master_mission,
                                     slave_mission, 'PICKLE', prod_dir,
                                     fine_int_xml, bbox_json, met_file), shell=True)

    # add master/slave ids and orbits to met JSON (per ASF request)
    master_ids = [i.replace(".zip", "") for i in ctx['master_zip_file']]
    slave_ids = [i.replace(".zip", "") for i in ctx['slave_zip_file']]
    master_rt = parse(os.path.join(prod_dir, "master.xml"))
    master_orbit_number = eval(master_rt.xpath('.//property[@name="orbitnumber"]/value/text()')[0])
    slave_rt = parse(os.path.join(prod_dir, "slave.xml"))
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

    # add dem_type
    md['dem_type'] = dem_type

    # write met json
    with open(met_file, 'w') as f: json.dump(md, f, indent=2)

    # generate dataset JSON
    ds_file = os.path.join(prod_dir, "{}.dataset.json".format(id))
    create_dataset_json(id, version, met_file, ds_file)
    
    # tar slc_pair product
    #check_call("tar cvf {}/{}.tbz2 --use-compress-program=pbzip2 {} {} {} {}".format(prod_dir,
    #                                                                                 prod_dir,
    #                                                                                 'PICKLE',
    #                                                                                 'master',
    #                                                                                 'geom_master',
    #                                                                                 'fine_coreg'), shell=True)

    # move slc_pair data dirs to product directory
    shutil.move('PICKLE', prod_dir)
    shutil.move('master', prod_dir)
    shutil.move('geom_master', prod_dir)
    shutil.move('fine_coreg', prod_dir)
    
    # write PROV-ES JSON
    #${BASE_PATH}/create_prov_es-create_interferogram.sh $id $project $master_orbit_file $slave_orbit_file \
    #                                                        ${dem_file}.xml $dem_file $WORK_DIR \
    #                                                        ${id}/${id}.prov_es.json > create_prov_es.log 2>&1
    
    # clean out SAFE directories and DEM files
    for i in chain(master_safe_dirs, slave_safe_dirs): shutil.rmtree(i)
    for i in glob("dem*"): os.unlink(i)


if __name__ == '__main__':
    try: status = main()
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)
