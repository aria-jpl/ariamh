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

    mgc_cmd = [
        "{}/makeGeocube.py".format(BASE_PATH), "-m", "reference",
        "-s", "secondary", "-o", "metadata.h5"
    ]
    mgc_cmd_line = " ".join(mgc_cmd)
    logger.info("Calling makeGeocube.py: {}".format(mgc_cmd_line))
    check_call(mgc_cmd_line, shell=True)


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
