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
import ifg_utils
from create_input_xml_alos2 import create_input_xml

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_alos2_ifg.log')


BASE_PATH = os.path.dirname(__file__)

IMG_RE=r'IMG-(\w{2})-ALOS(\d{6})(\d{4})-*'


def main():


    ''' Run the install '''
    wd = os.getcwd()
    
    
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

    xml_file = "alos2app_{}.xml".format(ifg_type)
    tmpl_file = "{}.tmpl".format(xml_file)

    start_subswath = 1
    end_subswath = 5
    
    ref_data_dir = os.path.join(wd, "reference")
    sec_data_dir = os.path.join(wd, "secondary")

    alos2_script_path = os.environ['INSAR_ZERODOP_SCR']
    print("alos2_script_path : {}".format(alos2_script_path))

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
    SNWE, snwe_arr = ifg_utils.get_SNWE_bbox(ref_bbox)
    logging.info("snwe_arr : {}".format(snwe_arr))
    logging.info("SNWE : {}".format(SNWE))
    
    preprocess_dem_file, geocode_dem_file, preprocess_dem_xml, geocode_dem_xml = ifg_utils.download_dem(SNWE)
   
    ''' This is already done, so commenting it for now 
    slcs = {"reference" : "{}".format(reference_slc), "secondary" : "{}".format(secondary_slc)}
    ifg_utils.unzip_slcs(slcs)
    '''

    ref_pol, ref_frame_arr = ifg_utils.get_pol_frame_info(ref_data_dir)
    sec_pol, sec_frame_arr = ifg_utils.get_pol_frame_info(sec_data_dir)

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

    cmd = ["python3", "{}/alos2app.py".format(os.environ['INSAR_ZERODOP_SCR']), "-i", "{}".format(xml_file), "-e", "coherence"]
    ifg_utils.run_command(cmd)

    cmd = ["python3", "{}/ion.py".format(os.environ['INSAR_ZERODOP_SCR']), "-i", "{}".format(xml_file)]
    ifg_utils.run_command(cmd)

    cmd = ["python3", "{}/alos2app.py".format(os.environ['INSAR_ZERODOP_SCR']), "-i", "{}".format(xml_file), "-s", "filter"]
    ifg_utils.run_command(cmd)

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
