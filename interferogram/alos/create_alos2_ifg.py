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
#import extract_alos2_md
import ifg_utils
from create_input_xml_alos2 import create_input_xml

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_alos2_ifg.log')


BASE_PATH = os.path.dirname(__file__)

IMG_RE=r'IMG-(\w{2})-ALOS(\d{6})(\d{4})-*'
IFG_ID_ALOS2_TMPL = "ALOS2-INSARZD-{}-{}-{}-{}"

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

    ifg_hash = ifg_utils.get_ifg_hash([reference_slc], [secondary_slc])

    ifg_md['full_idc_hash'] = ifg_hash
    ifg_md['dem_type'] = dem_type
    ifg_md['reference_slc'] = reference_slc
    ifg_md['secondary_slc'] = secondary_slc
    ifg_md["interferogram_type"] = ifg_type
    ifg_md["azimuth_looks"] = azimuth_looks
    ifg_md["range_looks"] = range_looks
    ifg_md["burst_overlap"] = burst_overlap
    ifg_md["filter_strength"] = filter_strength

    version = ifg_utils.get_version("ALOS2_IFG")
    if not version:
        version = "v1.0"
    
    xml_file = "alos2app_{}.xml".format(ifg_type)
    tmpl_file = "{}.tmpl".format(xml_file)

    start_subswath = 1
    end_subswath = 5

    ifg_md["start_subswath"] = start_subswath
    ifg_md["end_subswath"] = end_subswath
    
    ref_data_dir = os.path.join(wd, "reference")
    sec_data_dir = os.path.join(wd, "secondary")

    alos2_script_path = os.environ['ISCE_HOME']
    print("alos2_script_path : {}".format(alos2_script_path))

    os.chdir(wd)

    ''' Extrach Reference SLC Metadata '''
    ref_insar_obj = extract_alos2_md.get_alos2_obj(ref_data_dir)
    extract_alos2_md.create_alos2_md_isce(ref_insar_obj, "ref_alos2_md.json")
    #extract_alos2_md.create_alos2_md_bos(ref_data_dir, "ref_alos2_md2.json")

    ''' Extrach Reference SLC Metadata '''
    sec_insar_obj = extract_alos2_md.get_alos2_obj(sec_data_dir)
    extract_alos2_md.create_alos2_md_isce(sec_insar_obj, "sec_alos2_md.json")
    #extract_alos2_md.create_alos2_md_bos(sec_data_dir, "sec_alos2_md2.json")
    

    with open("ref_alos2_md.json") as f:
        ref_md = json.load(f)
    
    with open("sec_alos2_md.json") as f:
        sec_md = json.load(f)

    ref_md['location'] = ref_md.pop('geometry')
    sec_md['location'] = sec_md.pop('geometry')
    
    sat_direction = "D"
    direction = ref_md["flight_direction"]
    if direction.lower() == 'asc':
        sat_direction = "A"
    
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
   
    ''' This is already done, so commenting it for now '''
    slcs = {"reference" : "{}".format(reference_slc), "secondary" : "{}".format(secondary_slc)}
    ifg_utils.unzip_slcs(slcs)
    

    ref_pol, ref_frame_arr = ifg_utils.get_pol_frame_info(ref_data_dir)
    sec_pol, sec_frame_arr = ifg_utils.get_pol_frame_info(sec_data_dir)

    if ref_pol != sec_pol:
        raise Exception("REF Pol : {} is different than SEC Pol : {}".format(ref_pol, sec_pol))

    '''
    Logic for Fram datas
    '''

    ifg_md["polarization"] = ref_pol


    ''' Some Fake Data'''
    ifg_md['sensing_start'] = datetime.now().isoformat()
    

    tmpl_file = os.path.join(BASE_PATH, tmpl_file)
    print(tmpl_file)
    create_input_xml(tmpl_file, xml_file,
                     str(ref_data_dir), str(sec_data_dir),
                     str(preprocess_dem_file), str(geocode_dem_file), start_subswath, end_subswath, burst_overlap,
                     str(ref_pol), str(ref_frame_arr), str(sec_pol), str(sec_frame_arr), snwe_arr)


    alos2_start_time=datetime.now()
    logger.info("ALOS2 Start Time : {}".format(alos2_start_time)) 

    cmd = ["python3", "{}/applications/alos2App.py".format(os.environ['ISCE_HOME']), "{}".format(xml_file)]
    ifg_utils.run_command(cmd)
    '''
    cmd = ["python3", "{}/applications/ion.py".format(os.environ['ISCE_HOME']),  "{}".format(xml_file)]
    ifg_utils.run_command(cmd)

    cmd = ["python3", "{}/applications/alos2App.py".format(os.environ['ISCE_HOME']), "-i", "{}".format(xml_file), "-s", "filter"]
    ifg_utils.run_command(cmd)
    '''

    dt_string = datetime.now().strftime("%d%m%YT%H%M%S")

    ifg_md['sensing_stop'] = datetime.now().isoformat()
    ifg_hash = ifg_hash[0:4]
     
    #IFG_ID_ALOS2_TMPL = "ALOS2-IFG-{}-{}-{}-{}"
    
    id = IFG_ID_ALOS2_TMPL.format(sat_direction, dt_string, ifg_hash, version.replace('.', '_') )
    prod_dir = id
    logger.info("prod_dir : %s" %prod_dir)
    
    insar_dir = os.path.join(wd, "insar")

    os.chdir(wd)
    os.makedirs(prod_dir, 0o755)

    #Copy the product
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
