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
import isce_functions_alos2
import ifg_utils
from create_input_xml_alos2 import create_input_xml

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_alos2_ifg.log')


BASE_PATH = os.path.dirname(__file__)

IMG_RE=r'IMG-(\w{2})-ALOS(\d{6})(\d{4})-*'
IFG_ID_ALOS2_TMPL = "ALOS2-INSARZD-{}-{}-{}-{}"
SLC_FILTERS = ['IMG-HH', 'LED', 'TRL']

def create_product(id, wd):
    insar_dir = os.path.json(wd, "insar")
    product_dir = os.path.join(wd, id)

    

def main():


    ''' Run the install '''
    wd = os.getcwd()
    ifg_md = {}    
    
    
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
    
    ifg_type = ctx["ifg_type"]
    azimuth_looks = ctx["azimuth_looks"]
    range_looks = ctx["range_looks"]
    burst_overlap = ctx["burst_overlap"]
    filter_strength = ctx["filter_strength"]

    ref_slc_filelist = ifg_utils.get_zip_contents(reference_slc)
    sec_slc_filelist = ifg_utils.get_zip_contents(secondary_slc)

    #Unzip the slc files
    slcs = {"reference" : "{}".format(reference_slc), "secondary" : "{}".format(secondary_slc)}
    ifg_utils.unzip_slcs(slcs, SLC_FILTERS)

    ifg_hash = ifg_utils.get_ifg_hash([reference_slc], [secondary_slc])

    ifg_md['full_idc_hash'] = ifg_hash
    ifg_md['dem_type'] = dem_type
    ifg_md['reference_slc'] = reference_slc
    ifg_md['secondary_slc'] = secondary_slc
    ifg_md["interferogram_type"] = ifg_type
    ifg_md["azimuth_looks"] = int(azimuth_looks)
    ifg_md["range_looks"] = int(range_looks)
    ifg_md["burst_overlap"] = float(burst_overlap)
    ifg_md["filter_strength"] = float(filter_strength)

    version = ifg_utils.get_version("ALOS2_IFG")
    if not version:
        version = "v1.0"
   
    start_subswath = 1
    end_subswath = 5

    ifg_md["start_subswath"] = start_subswath
    ifg_md["end_subswath"] = end_subswath
    
    ref_data_dir = os.path.join(wd, "reference")
    sec_data_dir = os.path.join(wd, "secondary")

    alos2_script_path = os.environ['ISCE_HOME']
    print("alos2_script_path : {}".format(alos2_script_path))

    os.chdir(wd)

    ''' Extrach SLC Metadata '''
    ref_md = isce_functions_alos2.create_alos2_md_json(ref_data_dir)
    sec_md = isce_functions_alos2.create_alos2_md_json(sec_data_dir)

    ref_md_json = "ref_alos2_md.json"
    with open(ref_md_json, "w") as f:
        json.dump(ref_md, f, indent=2)
        f.close()

    sec_md_json = "sec_alos2_md.json"
    with open(sec_md_json, "w") as f:
        json.dump(sec_md, f, indent=2)
        f.close()

    ''' Extrach Reference SLC Metadata 
    isce_functions_alos2.create_alos2_md_isce(ref_data_dir, "ref_alos2_md.json")
    isce_functions_alos2.create_alos2_md_isce(sec_data_dir, "sec_alos2_md.json")
    with open("ref_alos2_md.json") as f:
        ref_md = json.load(f)
    with open("sec_alos2_md.json") as f:
        sec_md = json.load(f)
    '''

    ref_md['location'] = ref_md.pop('geometry')
    sec_md['location'] = sec_md.pop('geometry')
    
    sat_direction = "D"
    direction = ref_md["flight_direction"]
    if direction.lower() == 'asc':
        sat_direction = "A"
    dt_string = datetime.now().strftime("%d%m%YT%H%M%S")
    ifg_hash = ifg_hash[0:4]

    #Check if ifg_already exists
    id = IFG_ID_ALOS2_TMPL.format(sat_direction, dt_string, ifg_hash, version.replace('.', '_') )

    #id = "ALOS2-INSARZD-D-18042020T154753-4be9-v1_0"
    if ifg_utils.check_ifg_status(id, "grq"):
        print("{} Already Exists. Exiting ....".format(id))
        exit(0)


    ifg_md['satelite_direction'] = direction
    ref_orbit = ref_md["absolute_orbit"]
    sec_orbit = sec_md["absolute_orbit"]
    ifg_md["orbit"] = list(set([ref_orbit, sec_orbit]))

    ref_frame = int(ref_md["frame"])
    sec_frame = int(sec_md["frame"])

    if ref_frame != sec_frame:
        print("Reference Frame : {} is NOT same as Secondery Frame : {}".format(ref_frame, sec_frame))
        #raise Exception("Reference Frame : {} is NOT same as Secondery Frame : {}".format(ref_frame, sec_frame))

    ifg_md["frame"] = "{}".format(ref_frame)
    
    ref_bbox = ref_md['location']['coordinates'][0]
    sec_bbox = sec_md['location']['coordinates'][0]
    union_geojson = ifg_utils.get_union_geometry([ref_md['location'], sec_md['location']])
    ifg_md["union_geojson"] = union_geojson
    print(union_geojson)

    SNWE, snwe_arr = ifg_utils.get_SNWE_complete_bbox(ref_bbox, sec_bbox)
    ifg_md["SNWE"] = SNWE
    logging.info("snwe_arr : {}".format(snwe_arr))
    logging.info("SNWE : {}".format(SNWE))
    
    preprocess_dem_file, geocode_dem_file, preprocess_dem_xml, geocode_dem_xml = ifg_utils.download_dem(SNWE)
   

    ref_pol, ref_frame_arr = ifg_utils.get_pol_frame_info(ref_slc_filelist)
    sec_pol, sec_frame_arr = ifg_utils.get_pol_frame_info(sec_slc_filelist)

    if ref_pol != sec_pol:
        raise Exception("REF Pol : {} is different than SEC Pol : {}".format(ref_pol, sec_pol))

    if set(ref_frame_arr) != set(sec_frame_arr):
        raise Exception("REF Frame : {} is different than SEC Frame : {}".format(ref_frame_arr, sec_frame_arr))
    '''
    Logic for Fram datas
    '''

    ifg_md["polarization"] = ref_pol


    ''' Some Fake Data'''
    ifg_md['sensing_start'] = "%sZ" % datetime.utcnow().isoformat('T')
    
    xml_file = "alos2app_{}.xml".format(ifg_type)
    tmpl_file = "{}.tmpl".format(xml_file)

    tmpl_file = os.path.join(BASE_PATH, tmpl_file)
    print(tmpl_file)
    create_input_xml(tmpl_file, xml_file,
                     str(ref_data_dir), str(sec_data_dir),
                     str(preprocess_dem_file), str(geocode_dem_file), start_subswath, end_subswath, burst_overlap,
                     str(ref_pol), str(ref_frame_arr), str(sec_pol), str(sec_frame_arr), snwe_arr)


    alos2_start_time=datetime.now()
    logger.info("ALOS2 Start Time : {}".format(alos2_start_time)) 

    os.chdir(wd)
    cmd = ["python3", "{}/applications/alos2App.py".format(os.environ['ISCE_HOME']), "{}".format(xml_file), "{}".format("--steps")]
    ifg_utils.run_command(cmd)
    '''
    cmd = ["python3", "{}/applications/ion.py".format(os.environ['ISCE_HOME']),  "{}".format(xml_file)]
    ifg_utils.run_command(cmd)

    cmd = ["python3", "{}/applications/alos2App.py".format(os.environ['ISCE_HOME']), "-i", "{}".format(xml_file), "-s", "filter"]
    ifg_utils.run_command(cmd)
    '''

    ifg_md['sensing_stop'] = "%sZ" % datetime.utcnow().isoformat('T')
     
    prod_dir = id
    logger.info("prod_dir : %s" %prod_dir)
    
    insar_dir = os.path.join(wd, "insar")

    os.chdir(wd)
    os.makedirs(prod_dir, 0o755)

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
    check_call(alos2_cmd_line, shell=True)

    # chdir back up to work directory

    #Copy the producta
    for name in glob("{}/filt_diff_*".format(insar_dir)):
        logger.info("Copying {} to {}".format(os.path.join(insar_dir, name),  prod_dir))
        shutil.copy(os.path.join(insar_dir, name),  prod_dir)    

    for name in glob("{}/filt_*".format(insar_dir)):
        logger.info("Copying {} to {}".format(os.path.join(insar_dir, name),  prod_dir))
        shutil.copy(os.path.join(insar_dir, name),  prod_dir)

    for name in glob("{}/diff_*".format(insar_dir)):
        logger.info("Copying {} to {}".format(os.path.join(insar_dir, name),  prod_dir))
        shutil.copy(os.path.join(insar_dir, name),  prod_dir)


    for name in glob("{}/*.slc.par.xml".format(insar_dir)):
        logger.info("Copying {} to {}".format(os.path.join(insar_dir, name),  prod_dir))
        shutil.copy(os.path.join(insar_dir, name),  prod_dir)

    for name in glob("{}/*.xml".format(insar_dir)):
        logger.info("Copying {} to {}".format(os.path.join(insar_dir, name),  prod_dir))
        shutil.copy(os.path.join(insar_dir, name),  prod_dir)

    shutil.copyfile("_context.json", os.path.join(prod_dir,"{}.context.json".format(id)))

    # generate met file
    met_file = os.path.join(prod_dir, "{}.met.json".format(id))
    with open(met_file, 'w') as f: json.dump(ifg_md, f, indent=2)

    # generate dataset JSON
    ds_file = os.path.join(prod_dir, "{}.dataset.json".format(id))
    logger.info("creating dataset file : %s" %ds_file)
    ifg_utils.create_dataset_json(id, version, met_file, ds_file)
 
    #alos2_packagina(id)


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
