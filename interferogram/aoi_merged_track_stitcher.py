#!/usr/bin/env python3
import sys
import os
import shutil
import json
import re
import hashlib
import glob
from .ifg_stitcher import IfgStitcher
from .enumerate_stitch_cfgs import *
from utils.createImage import createImage


import matplotlib
matplotlib.use('Agg')
# from matplotlib import pyplot as plt


def order_gunw_filenames(ls):
    '''
    :param ls: List[str]: list of gunw file names
    ex. s3://s3-us-west-2.amazonaws.com:80/aria-ops-dataset-bucket/datasets/interferogram/v2.0.0/2018/11/28/S1-GUNW-...
    :return: List[str] ordered list of GUNWs, ordered by first timestamp, also add + '/merged/filt_topophase.unw.geo'
    '''
    regex_string = r'([0-9]{8}T[0-9]{6})'
    localize_products = [{
        'file': p.split('/')[-1],
        'date': re.search(regex_string, p).group(1),
    } for p in ls]
    sorted_localize_producted = sorted(localize_products, key=lambda i: i['date'])
    sorted_localize_producted = [[p['file'] + '/merged/filt_topophase.unw.geo'] for p in sorted_localize_producted]
    return sorted_localize_producted


def get_min_max_timestamp(ls):
    '''
    :param ls: List[str]: list of gunw file names
    ex. s3://s3-us-west-2.amazonaws.com:80/aria-ops-dataset-bucket/datasets/interferogram/v2.0.0/2018/11/28/...
        .../S1-GUNW-MERGED_RM_M1S2_TN042_20181210T140904-20181116T140812_s123-poeorb-96da
    :return: Str, Str: string timestamps YYYYMMDDTHHMMSS (ex. 20190506T103055)
                       first timestamp is the min of the master timestamp
                       second timestamp is the max of the slave timestamp
    '''
    regex_string = r'([0-9]{8}T[0-9]{6})-([0-9]{8}T[0-9]{6})'
    master_timestamps = []
    slave_timestamps = []
    for product in ls:
        regex_matches = re.search(regex_string, product)
        slave_timestamps.append(regex_matches.group(2))
        master_timestamps.append(regex_matches.group(1))
    return min(slave_timestamps), max(master_timestamps)


def generate_list_dataset_ids(ls):
    '''
    :param ls: list[str]  list of s3 download urls containing GUNW ids
    :return: list[str] list of MERGED GUNW ids
    '''
    return [re.search(r'(S1-GUNW-MERGED.+)', url).group(1) for url in ls]


def generate_4digit_hash(datasets_ls):
    '''
    :param ls: list[str] list of MERGED GUNW ids
    :return: str, first 4 characters of md5 generated hash from jsonified input list

    description:
        datasets_ls is the list of MERGED GUNW ids, we will jsonify it and run it through the hashlib (md5)
    '''
    m = hashlib.md5()
    datasets_json = json.dumps(datasets_ls).encode('utf-8')
    m.update(datasets_json)
    return m.hexdigest()[:4]


def generate_files_to_move_to_dataset_directory(list_extra_products):
    '''
    description:
        first generates list of all files to move
        one by one moves the files to directory

    :param list_extra_products: list[str], default value ["los.rdr.geo"],
           specified in TOSCA inputs: space separated list of products to process, e.g. los.rdr.geo
    :param destination: where we want to copy the files os.cwd() + dataset_directory
    :return: list[str] list of all files to move
    '''
    files_to_move = []
    file_types = list_extra_products + ['filt_topophase', 'phsig']  # list of all file types to move to the dataset dir
    for file_type in file_types:
        pattern = file_type + '.*'  # capture all files starting with specified file names
        files_to_move += glob.glob(pattern)
    print('identified list of files to move: {}'.format(files_to_move))
    return files_to_move


def move_files_to_dataset_directory(ls, destination):
    '''
    :param ls: list[str] files we want to move to the dataset directory, ie. destination
    :param destination: str, dataset directory
    :return: void
    '''
    for file in ls:
        shutil.move(file, destination)
    return True


def run_stitcher(inps):
    st = IfgStitcher()
    try:
        # the stitcher code doesnt raise a proper exception so it will be raised here just to be safe
        st.stitch(inps)  # this will generate a bunch of .geo, .vrt and .xml files in your workdir
    except Exception as e:
        print("Something happened in the IfgStitcher.stitch() code, need to contact author of code")
        print(e)
        raise Exception("Something happened in the IfgStitcher.stitch() code, need to contact author of code")


# copied from stitch_ifgs.get_union_polygon()
# def get_union_polygon(ds_files):
#     """Get GeoJSON polygon of union of IFGs."""
#
#     geom_union = None
#     for ds_file in ds_files:
#         with open(ds_file) as f:
#             ds = json.load(f)
#         geom = ogr.CreateGeometryFromJson(json.dumps(ds['location'], indent=2, sort_keys=True))
#         if geom_union is None:
#             geom_union = geom
#         else:
#             geom_union = geom_union.Union(geom)
#     return json.loads(geom_union.ExportToJson()), geom_union.GetEnvelope()


if __name__ == '__main__':
    cwd = os.getcwd()

    ctx_file = os.path.abspath('_context.json')  # get context
    if not os.path.exists(ctx_file):
        raise RuntimeError("Failed to find _context.json.")

    with open(ctx_file) as f:
        ctx = json.load(f)

    localize_products = ctx['job_specification']['params'][1]['value'][0]
    print('localized products: {}'.format(json.dumps(localize_products, indent=2)))
    input_files = order_gunw_filenames(localize_products)
    print('ordered input files: {}'.format(json.dumps(input_files, indent=2)))

    # getting timestamps to name the new dataset
    master_timestamp, slave_timestamp = get_min_max_timestamp(localize_products)
    track_number = re.search(r'S1-GUNW-MERGED_RM_.+_TN([0-9].+?)_', localize_products[0]).group(1)
    list_datatset_ids = generate_list_dataset_ids(localize_products)
    four_digit_hash = generate_4digit_hash(list_datatset_ids)

    stitch_dataset_id = 'S1-GUNW-MERGED_TN{track}_{master_end_time}-{slave_start_time}-poeorb-{hash}'
    stitch_dataset_id = stitch_dataset_id.format(master_end_time=master_timestamp, slave_start_time=slave_timestamp,
                                                 track=track_number, hash=four_digit_hash)
    print('stitched gunw id: %s' % stitch_dataset_id)

    dataset_dir = os.path.join(cwd, stitch_dataset_id)
    if not os.path.exists(dataset_dir):  # generating the dataset directory so verdi can publish when done
        os.mkdir(dataset_dir)
        print('created dataset directory: %s' % dataset_dir)

    outname = 'filt_topophase.unw.geo'  # main outputted file name from the stitcher
    extra_products = ctx.get('extra_products', [])
    extra_products = [p.strip() for p in extra_products.split(' ')]  # turning space split extra products in list

    # these are the inputs needed to run the scientist's ifg stitcher function
    stitcher_inputs = {
        'direction': 'along',
        'extra_products': extra_products,
        'filenames': input_files,
        'outname': outname,
    }
    print('stitcher inputs: {}'.format(json.dumps(stitcher_inputs, indent=2)))

    run_stitcher(stitcher_inputs)  # the function will exit out if the stitching fails
    print("Stitcher completed, outputted .geo, .xrt and .xml files")

    dataset_files = generate_files_to_move_to_dataset_directory(extra_products)
    move_files_to_dataset_directory(dataset_files)  # moving all proper files to dataset dir
    print('files moved to {}: {}'.format(dataset_dir, json.dumps(dataset_files, indent=2)))

    # using the get_stitch_cfgs function from enumerate_stitch_cfgs.py
    stitch_cfgs = get_stitch_cfgs(ctx_file)
    print(stitch_cfgs)

    # create browse images
    os.chdir(dataset_dir)
    mdx_app_path = "{}/applications/mdx.py".format(os.environ['ISCE_HOME'])
    mdx_path = "{}/bin/mdx".format(os.environ['ISCE_HOME'])
    unw_file = "filt_topophase.unw.geo"

    # ** uses the mdx.py **
    createImage("{} -P {}".format(mdx_app_path, unw_file), unw_file)

    # TODO:
    #   create _dataset.json https://github.com/aria-jpl/ariamh/blob/develop/interferogram/stitch_ifgs.py#L62-L91
    #   this regex works: S1-GUNW-MERGED_TN.*?_.*?-(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<rest>.+)
    # TODO: create met.json file
    #   CREATE UTILITY FUNCTIONS IN utils.py AND USE **kwargs INPUTS
    #   add stitch_count (or scene_count), int: is len(localize_urls) or 1 as default
    #   add polygon to dataset.json
    #   the <dataset_id>.context.json is the same as _context.json

    sys.exit(0)
