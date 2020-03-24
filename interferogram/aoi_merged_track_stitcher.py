#!/usr/bin/env python3
from __future__ import absolute_import
import sys
import os
import glob
import shutil
import json
import math
import numpy as np
import re
import hashlib
from datetime import datetime

from .ifg_stitcher import IfgStitcher
from utils.create_datasets import create_dataset_json
from utils.createImage import createImage

from osgeo import ogr, osr
import matplotlib
matplotlib.use('Agg')


def get_version():
    # dataset version: stored in conf/dataset_versions.json
    ds_vers_cfg = os.path.normpath(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '..', 'conf', 'dataset_versions.json'))
    with open(ds_vers_cfg) as f:
        ds_vers = json.load(f)
    return ds_vers['S1-GUNW-MERGED-STITCHED'] + 'b'


def order_gunw_filenames(ls):
    '''
    :param ls: List[str]: list of gunw file names
    ex. s3://s3-us-west-2.amazonaws.com:80/aria-ops-dataset-bucket/datasets/interferogram/v2.0.0/2018/11/28/S1-GUNW-...
    :return: List[str] ordered list of GUNWs, ordered by first timestamp, also add + '/merged/filt_topophase.unw.geo'
    '''
    localize_products = []
    for p in ls:
        file_name = p.split('/')[-1]
        try:
            date_match = re.search(r'([0-9]{8}T[0-9]{6})', p).group(1)
        except Exception as e:
            date_match = re.search(r'([0-9]{8})', p).group(1)

        localize_products.append({
            'file': file_name,
            'date': date_match,
        })
    sorted_localize_producted = sorted(localize_products, key=lambda i: i['date'])
    sorted_localize_producted = [[p['file'] + '/merged/filt_topophase.unw.geo'] for p in sorted_localize_producted]
    return sorted_localize_producted


def get_master_slave_scene_count():
    '''
    Gets the master scene count and slave scene count from each met.json file in localized directories
    :return: list[str], int, list[str], int
    '''
    localized_datasets = [x for x in os.listdir('.') if x.startswith('S1-GUNW-MERGED')]
    master_scenes = set()
    slave_scenes = set()

    for localized_dataset_dir in localized_datasets:
        met_json_file_path = localized_dataset_dir + '/' + localized_dataset_dir + '.met.json'

        with open(met_json_file_path) as f:
            met_json_metadata = json.load(f)
            secondary_scenes = met_json_metadata.get('secondary_scenes', [])
            reference_scenes = met_json_metadata.get('reference_scenes', [])
            [master_scenes.add(scene) for scene in reference_scenes]
            [slave_scenes.add(scene) for scene in secondary_scenes]

    print('master scenes: {}'.format(json.dumps(list(master_scenes), indent=2)))
    print('slave scenes: {}'.format(json.dumps(list(slave_scenes), indent=2)))
    return list(master_scenes), len(master_scenes), list(slave_scenes), len(slave_scenes)


def get_min_max_timestamp(ls):
    '''
    :param ls: List[str]: list of gunw file names
    ex. s3://s3-us-west-2.amazonaws.com:80/aria-ops-dataset-bucket/datasets/interferogram/v2.0.0/2018/11/28/...
        .../S1-GUNW-MERGED_RM_M1S2_TN042_20181210T140904-20181116T140812_s123-poeorb-96da
    :return: Str, Str: string timestamps YYYYMMDDTHHMMSS (ex. 20190506T103055)
                       first timestamp is the min of the master timestamp
                       second timestamp is the max of the slave timestamp
    '''
    master_timestamps = []
    slave_timestamps = []
    for product in ls:
        try:
            regex_matches = re.search(r'([0-9]{8}T[0-9]{6})-([0-9]{8}T[0-9]{6})', product)
            slave_timestamps.append(regex_matches.group(2))
            master_timestamps.append(regex_matches.group(1))
        except Exception as e:
            regex_matches = re.search(r'([0-9]{8})-([0-9]{8})', product)
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

    :param list_extra_products: list[str], default value ['los.rdr.geo'],
           specified in TOSCA inputs: space separated list of products to process, e.g. los.rdr.geo
    :param destination: where we want to copy the files os.cwd() + dataset_directory
    :return: list[str] list of all files to move
    '''
    files_to_move = []
    file_types = list_extra_products + ['filt_topophase', 'phsig']  # list of all file types to move to the dataset dir
    for file_type in file_types:
        pattern = file_type + '*'  # capture all files starting with specified file names
        files_to_move += glob.glob(pattern)
    print('identified list of files to move: {}'.format(files_to_move))
    return files_to_move


def move_files_to_dataset_directory(ls, destination):
    '''
    :param ls: list[str] files we want to move to the dataset directory, ie. destination
    :param destination: str, dataset directory
    :param *args are additional files you want to move
    :return: void
    '''
    for file in ls:
        new_file_path = os.path.join(destination, file)
        if os.path.isfile(new_file_path):
            os.remove(new_file_path)
        shutil.move(file, destination)
    return True


def run_stitcher(inps):
    st = IfgStitcher()
    try:
        # the stitcher code doesnt raise a proper exception so it will be raised here just to be safe
        st.stitch(inps)  # this will generate a bunch of .geo, .vrt and .xml files in your workdir
    except Exception as e:
        print('Something happened in the IfgStitcher.stitch() code, need to contact author of code')
        print(e)
        raise Exception('Something happened in the IfgStitcher.stitch() code, need to contact author of code')


def generate_list_of_gunw_merged_dataset_met_files(localize_urls, cur_dir=os.getcwd()):
    '''
    takes context.json and returns a list of S1-GUNW-MERGED_RM_<filler>_.json dataset files
    the output will be fed into another function to spit out the union polygon to be added into datasets.json
    :param: list[str], list of download urls, containing the S1 GUNW MERGED ID
    :param: cur_dir: current_directory (os.getcwd())
    :return: list[str], list of directories
    '''
    list_dataset_json_files = []
    list_met_json_files = []
    for url in localize_urls:
        dataset_id = url.split('/')[-1]

        dataset_json_file = '%s.dataset.json' % dataset_id
        met_json_file = '%s.met.json' % dataset_id

        dataset_json_path = os.path.join(cur_dir, dataset_id, dataset_json_file)
        met_json_path = os.path.join(cur_dir, dataset_id, met_json_file)

        list_dataset_json_files.append(dataset_json_path)
        list_met_json_files.append(met_json_path)
    return list_dataset_json_files, list_met_json_files


# copied from stitch_ifgs.get_union_polygon()
def get_union_polygon(ds_files):
    '''Get GeoJSON polygon of union of IFGs.'''

    geom_union = None
    for ds_file in ds_files:
        with open(ds_file) as f:
            ds = json.load(f)
        geom = ogr.CreateGeometryFromJson(json.dumps(ds['location'], indent=2, sort_keys=True))
        if geom_union is None:
            geom_union = geom
        else:
            geom_union = geom_union.Union(geom)
    return json.loads(geom_union.ExportToJson()), geom_union.GetEnvelope()


# copied straight from stitch_ifgs.py, but altered it a little
def generate_met_json_file(dataset_id, version, env, starttime, endtime, met_files, met_json_filename, direction,
                           ref_scenes, sec_scenes):
    '''
    :param dataset_id: string, id of the stitched GUNW
    :param version: string, version of GUNW
    :param env: list[float], corner coordinates of stitch gunw(?)
    :param starttime: string, timestamps of job start time
    :param endtime: string, timestamps of job end time
    :param met_files: list[str], list of met filepaths the function will read out of
    :param met_json_filename: met json filename
    :param direction: string, direction of stitcher
    :return: void
    '''
    # build met
    bbox = [
        [env[3], env[0]],
        [env[3], env[1]],
        [env[2], env[1]],
        [env[2], env[0]],
    ]
    met = {
        'stitch_direction': direction,
        'product_type': 'interferogram',
        'refbbox': [],
        'esd_threshold': [],
        'frame_id': [],
        'temporal_span': None,
        'track_number': None,
        'archive_filename': dataset_id,
        'dataset_type': 'slc',
        'tile_layers': ['amplitude', 'displacement'],
        'latitude_index_min': int(math.floor(env[2] * 10)),
        'latitude_index_max': int(math.ceil(env[3] * 10)),
        'parallel_baseline': [],
        'url': [],
        'doppler': [],
        'version': [],
        'orbit_type': [],
        'frame_number': None,
        'bbox': bbox,
        'ogr_bbox': [[x, y] for y, x in bbox],
        'orbit_number': [],
        'input_file': 'ifg_stitch.json',
        'perpendicular_baseline': [],
        'orbit_repeat': [],
        'sensing_stop': endtime,
        'polarization': [],
        'scene_count': 0,
        'beam_id': None,
        'sensor': [],
        'look_direction': [],
        'platform': [],
        'starting_range': [],
        'frame_name': [],
        'tiles': True,
        'sensing_start': starttime,
        'beam_mode': [],
        'image_corners': [], # may keep
        'prf': [],
        'orbit_direction': [],
        'reference_scenes': ref_scenes,
        'secondary_scenes': sec_scenes,
        "sha224sum": hashlib.sha224(str.encode(os.path.basename(met_json_filename))).hexdigest(),
    }

    # collect values
    set_params = ('esd_threshold', 'frame_id', 'parallel_baseline', 'doppler', 'orbit_type', 'orbit_number',
                  'perpendicular_baseline', 'orbit_repeat', 'polarization', 'sensor', 'look_direction', 'platform',
                  'starting_range', 'beam_mode', 'prf')
    single_params = ('temporal_span', 'track_number', 'orbit_direction')
    list_params = ('platform', 'perpendicular_baseline', 'parallel_baseline')
    mean_params = ('perpendicular_baseline', 'parallel_baseline')

    print("list individual GUNW merged met.json files: {}".format(met_files))

    for i, met_file in enumerate(met_files):
        with open(met_file) as f:
            md = json.load(f)
        for param in set_params:
            # logger.info("param: {}".format(param))
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
    print('generated met.json object: {}'.format(json.dumps(met, indent=2)))

    with open(met_json_filename, 'w') as f:  # writing the met.json
        met_json = json.dumps(met, indent=2)
        f.write(met_json)
    return True


if __name__ == '__main__':
    VERSION = get_version()
    START_TIME = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    DIRECTION = 'along'

    cwd = os.getcwd()
    ctx_file = os.path.abspath('_context.json')  # get context
    if not os.path.exists(ctx_file):
        raise RuntimeError('Failed to find _context.json.')

    with open(ctx_file) as f:
        ctx = json.load(f)

    localize_products = ctx['job_specification']['params'][1]['value']
    print('localized products: {}'.format(json.dumps(localize_products, indent=2)))
    input_files = order_gunw_filenames(localize_products)
    print('ordered input files: {}'.format(json.dumps(input_files, indent=2)))

    # getting timestamps to name the new dataset
    master_timestamp, slave_timestamp = get_min_max_timestamp(localize_products)
    track_number = re.search(r'S1-GUNW-MERGED_RM_.+_TN([0-9].+?)_', localize_products[0]).group(1)
    list_datatset_ids = generate_list_dataset_ids(localize_products)
    four_digit_hash = generate_4digit_hash(list_datatset_ids)

    master_scenes, master_count, slave_scenes, slave_count = get_master_slave_scene_count()

    stitch_dataset_id = 'S1-GUNW-MERGED_RM_M{master_count}S{slave_count}_TN{track}_{master_end_time}-{slave_start_time}-poeorb-{hash}'
    stitch_dataset_id = stitch_dataset_id.format(master_end_time=master_timestamp, slave_start_time=slave_timestamp,
                                                 master_count=master_count, slave_count=slave_count,
                                                 track=track_number, hash=four_digit_hash)
    print('stitched gunw id: %s' % stitch_dataset_id)

    outname = 'filt_topophase.unw.geo'  # main outputted file name from the stitcher
    extra_products = ctx.get('extra_products', [])
    extra_products = [p.strip() for p in extra_products.split(' ')]  # turning space split extra products in list

    # these are the inputs needed to run the scientist's ifg stitcher function
    stitcher_inputs = {
        'direction': DIRECTION,
        'extra_products': extra_products,
        'filenames': input_files,
        'outname': outname,
    }
    stitcher_inputs_filename = 'inputs.json'
    with open(stitcher_inputs_filename, 'w') as f:  # scientists want this json file in the dataset directory
        f.write(json.dumps(stitcher_inputs, indent=2))
    print('stitcher inputs: {}'.format(json.dumps(stitcher_inputs, indent=2)))

    list_gunws_dataset_json, list_gunws_met_json = generate_list_of_gunw_merged_dataset_met_files(localize_products)
    print('List of GUNW MERGED dataset.json files: {}'.format(json.dumps(list_gunws_dataset_json, indent=2)))

    run_stitcher(stitcher_inputs)  # the function will exit out if the stitching fails
    print('Stitcher completed, outputted .geo, .xrt and .xml files')

    # create dataset directory
    dataset_dir = os.path.join(cwd, stitch_dataset_id)
    if not os.path.exists(dataset_dir):  # generating the dataset directory so verdi can publish when done
        os.mkdir(dataset_dir)
        os.mkdir(dataset_dir + '/merged')
        print('created dataset directory: %s' % dataset_dir)

    dataset_files = generate_files_to_move_to_dataset_directory(extra_products)
    dataset_files.append(stitcher_inputs_filename)
    move_files_to_dataset_directory(dataset_files, dataset_dir + '/merged')  # moving all proper files to dataset dir
    print('files moved to {}: {}'.format(dataset_dir, json.dumps(dataset_files, indent=2)))

    # create browse images
    os.chdir(dataset_dir)
    mdx_app_path = '{}/applications/mdx.py'.format(os.environ['ISCE_HOME'])
    mdx_path = '{}/bin/mdx'.format(os.environ['ISCE_HOME'])
    unw_file = 'filt_topophase.unw.geo'
    createImage('{} -P {}'.format(mdx_app_path, 'merged/' + unw_file), unw_file)  # ** uses the mdx.py **

    union_polygon = get_union_polygon(list_gunws_dataset_json)
    union_polygon_coordinates = union_polygon[0]
    image_corners = union_polygon[1]

    print('union polygon: {}'.format(json.dumps(union_polygon, indent=2)))

    # CREATING DATASET.JSON FILE
    END_TIME = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    create_dataset_json(stitch_dataset_id, VERSION, START_TIME, end_time=END_TIME, location=union_polygon_coordinates)

    # CREATING MET.JSON METADATA
    # create_met_json(id, version, env, starttime, endtime, met_files, met_json_file, direction)
    met_json_filename = '%s.met.json' % stitch_dataset_id

    generate_met_json_file(stitch_dataset_id, VERSION, image_corners, START_TIME, END_TIME, list_gunws_met_json,
                    met_json_filename, DIRECTION, master_scenes, slave_scenes)
    print("wrote met.json file: %s" % os.path.join(cwd, met_json_filename))

    sys.exit(0)
