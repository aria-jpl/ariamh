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
import extract_alos2_md
from create_input_xml import create_input_xml

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_alos2_ifg.log')


BASE_PATH = os.path.dirname(__file__)

IMG_RE=r'IMG-(\w{2})-ALOS(\d{6})(\d{4})-*'

def create_product(id):
    pass

def alos2_packaging(id):
    # create alos2 packaging
    alos2_prod_file = "{}.nc".format(id)

    with open(os.path.join(BASE_PATH, "alos2_groups.json")) as f:
        alos2_cfg = json.load(f)
    alos2_cfg['filename'] = alos2_prod_file
    with open('alos2_groups.json', 'w') as f:
        json.dump(alos2_cfg, f, indent=2, sort_keys=True)
    alos2_cmd = [
        "{}/alos2_packaging.py".format(BASE_PATH)
    ]
    alos2_cmd_line = " ".join(alos2_cmd)
    logger.info("Calling alos2_packaging.py: {}".format(alos2_cmd_line))
    check_call(also2_cmd_line, shell=True)

    # chdir back up to work directory
    os.chdir(cwd) # create standard product packaging

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
    with open(file_name, 'r') as f:
        datafile = f.readlines()
    for line in datafile:
        if msg in line:
            # found = True # Not necessary
            return True, line
    return False, None

def checkBurstError():
    msg = "cannot continue for interferometry applications"

    found, line = fileContainsMsg("alos2app.log", msg)
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

def get_SNWE_bbox(bbox):
    lons = []
    lats = []

    for pp in bbox:
        lons.append(pp[0])
        lats.append(pp[1])

    return get_SNWE(min(lons), max(lons), min(lats), max(lats))

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

    checkBurstError()

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

def main():


    ''' Run the install '''
    wd = os.getcwd()
    
    new_dir= "{}/src".format(BASE_PATH)
    logging.info(new_dir)
    os.chdir(new_dir)
    cmd = "./install.sh"
    os.system(cmd)

    os.chdir(wd)
    cmd= ["pwd"]
    run_command(cmd)
    
    ''' Get the informations from _context file '''
    ctx_file = os.path.abspath('_context.json')
    if not os.path.exists(ctx_file):
        raise RuntimeError("Failed to find _context.json.")
    with open(ctx_file) as f:
        ctx = json.load(f)

    # save cwd (working directory)
    complete_start_time=datetime.now()
    logger.info("Alos2 start Time : {}".format(complete_start_time))

    dem_type = ctx['dem_type']
    reference_slc = ctx['reference_product']
    secondary_slc = ctx['secondary_product']
    SNWE = ctx['SNWE']
    
    ref_data_dir = os.path.join(wd, "reference")
    sec_data_dir = os.path.join(wd, "secondary")

    os.chdir(wd)

    ''' Extrach Reference SLC Metadata'''
    ref_insar_obj = extract_alos2_md.get_alos2_obj(ref_data_dir)
    extract_alos2_md.create_alos2_md_isce(ref_insar_obj, "ref_alos2_md.json")
    #extract_alos2_md.create_alos2_md_bos(ref_data_dir, "ref_alos2_md2.json")

    ''' Extrach Reference SLC Metadata'''
    sec_insar_obj = extract_alos2_md.get_alos2_obj(sec_data_dir)
    extract_alos2_md.create_alos2_md_isce(sec_insar_obj, "sec_alos2_md.json")
    #extract_alos2_md.create_alos2_md_bos(sec_data_dir, "sec_alos2_md2.json")

    with open("ref_alos2_md.json") as f:
        ref_md = json.load(f)
    
    with open("sec_alos2_md.json") as f:
        sec_md = json.load(f)

    ref_bbox = ref_md['geometry']['coordinates'][0]
    SNWE, snwe_arr = get_SNWE_bbox(ref_bbox)
    #SNWE = "14 25 -109 -91"
    logging.info("snwe_arr : {}".format(snwe_arr))
    logging.info("SNWE : {}".format(SNWE))
    
    preprocess_dem_file, geocode_dem_file, preprocess_dem_xml, geocode_dem_xml = download_dem(SNWE)
   
    ''' This is already done, so commenting it for now 
    slcs = {"reference" : "0000230036_001001_ALOS2227337160-180808.zip", "secondary" : "0000230039_001001_ALOS2235617160-181003.zip"}
    unzip_slcs(slcs)
    '''

    ifg_type = "scansar"
    xml_file = "alos2app_scansar.xml"
    tmpl_file = "alos2app_scansar.xml.tmpl"
    start_subswath = 1
    end_subswath = 5
    burst_overlap = 85.0
   
    ref_pol, ref_frame_arr = get_pol_frame_info(ref_data_dir)
    sec_pol, sec_frame_arr = get_pol_frame_info(sec_data_dir)

    if ref_pol != sec_pol:
        raise Exception("REF Pol : {} is different than SEC Pol : {}".format(ref_pol, sec_pol))

    '''
    Logic for Fram datas
    '''


    tmpl_file = os.path.join(BASE_PATH, tmpl_file)
    print(tmpl_file)
    create_input_xml(tmpl_file, xml_file,
                     str(ref_data_dir), str(sec_data_dir),
                     str(preprocess_dem_file), str(geocode_dem_file), start_subswath, end_subswath, burst_overlap,
                     str(ref_pol), str(ref_frame_arr), str(sec_pol), str(sec_frame_arr), snwe_arr)


    alos2_start_time=datetime.now()
    logger.info("ALOS2 Start Time : {}".format(alos2_start_time)) 

    cmd = ["python3", "{}/scripts/alos2app.py".format(BASE_PATH), "-i", "{}".format(xml_file), "-e", "coherence"]
    run_command(cmd)

    cmd = ["python3", "{}/scripts/ion.py".format(BASE_PATH), "-i", "{}".format(xml_file)]
    run_command(cmd)

    cmd = ["python3", "{}/scripts/alos2app.py".format(BASE_PATH), "-i", "{}".format(xml_file), "-s", "filter"]
    run_command(cmd)

    dt_string = datetime.now().strftime("%d%m%YT%H%M%S")
    id = "ALOS2_{}_{}".format(dem_type, dt_string)

    create_product(id)
    
    alos2_packaging(id)


if __name__ == '__main__':
    complete_start_time=datetime.now()
    logger.info("TopsApp End Time : {}".format(complete_start_time))
    cwd = os.getcwd()

    ctx_file = os.path.abspath('_context.json')
    if not os.path.exists(ctx_file):
        raise RuntimeError("Failed to find _context.json.")
    with open(ctx_file) as f:
        ctx = json.load(f)

    main()
