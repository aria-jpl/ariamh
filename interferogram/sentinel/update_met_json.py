#!/usr/bin/env python3
import os, sys, json, re, math, logging, traceback, pickle, hashlib
from lxml.etree import parse
from osgeo import gdal
import numpy as np

import isce
from iscesys.Component.ProductManager import ProductManager as PM
from isceobj.Orbit.Orbit import Orbit

from utils.time_utils import getTemporalSpanInDays


gdal.UseExceptions() # make GDAL raise python exceptions


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('update_met_json')


SENSING_RE = re.compile(r'(S1-IFG_.*?_(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})-(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2}).*?orb)')
MISSION_RE = re.compile(r'^S1(\w)$')


def get_raster_corner_coords(vrt_file):
    """Return raster corner coordinates."""

    # go to directory where vrt exists to extract from image
    cwd =os.getcwd()
    data_dir = os.path.dirname(os.path.abspath(vrt_file))
    os.chdir(data_dir)

    # extract geo-coded corner coordinates
    ds = gdal.Open(os.path.basename(vrt_file))
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    ext = []
    lon_arr = [0, cols]
    lat_arr = [0, rows]
    for px in lon_arr:
        for py in lat_arr:
            lon = gt[0] + (px * gt[1]) + (py * gt[2])
            lat = gt[3] + (px * gt[4]) + (py * gt[5])
            ext.append([lat, lon])
        lat_arr.reverse()
    os.chdir(cwd)
    return ext


def load_product(int_file):
    """Load product from fine interferogram xml file."""

    pm = PM()
    pm.configure()
    prod = pm.loadProduct(int_file)
    return prod


def get_orbit():
    """Return orbit object."""

    orb = Orbit()
    orb.configure()
    return orb


def get_aligned_bbox(prod, orb):
    """Return estimate of 4 corner coordinates of the
       track-aligned bbox of the product."""

    # create merged orbit
    burst = prod.bursts[0]

    #Add first burst orbit to begin with
    for sv in burst.orbit:
         orb.addStateVector(sv)

    ##Add all state vectors
    for bb in prod.bursts:
        for sv in bb.orbit:
            if (sv.time< orb.minTime) or (sv.time > orb.maxTime):
                orb.addStateVector(sv)
        bb.orbit = orb

    # extract bbox
    ts = [prod.sensingStart, prod.sensingStop]
    rngs = [prod.startingRange, prod.farRange]
    pos = []
    for tim in ts:
        for rng in rngs:
            llh = prod.orbit.rdr2geo(tim, rng, height=0.)
            pos.append(llh)
    pos = np.array(pos)
    bbox = pos[[0, 1, 3, 2], 0:2]
    return bbox.tolist()


def update_met_json(orbit_type, scene_count, swath_num, master_mission,
                    slave_mission, pickle_dir, int_file, vrt_file, 
                    xml_file, json_file):
    """Write product metadata json."""

    xml_file = os.path.abspath(xml_file)
    with open(xml_file) as f:
        doc = parse(f)
    coordinate1  =  doc.xpath('.//component[@name="coordinate1"]')[0]
    width = float(coordinate1.xpath('.//property[@name="size"]/value')[0].text)
    startLon = float(coordinate1.xpath('.//property[@name="startingvalue"]/value')[0].text)
    deltaLon = float(coordinate1.xpath('.//property[@name="delta"]/value')[0].text)
    endLon = startLon + deltaLon*width
    coordinate2  =  doc.xpath('.//component[@name="coordinate2"]')[0]
    length = float(coordinate2.xpath('.//property[@name="size"]/value')[0].text)
    startLat = float(coordinate2.xpath('.//property[@name="startingvalue"]/value')[0].text)
    deltaLat = float(coordinate2.xpath('.//property[@name="delta"]/value')[0].text)
    endLat = startLat + deltaLat*length
    minLat = min(startLat,endLat)
    maxLat = max(startLat,endLat)
    minLon = min(startLon,endLon)
    maxLon = max(startLon,endLon)
    match = SENSING_RE.search(xml_file)
    if not match:
        raise RuntimeError("Failed to extract sensing times: %s" % xml_file)
    archive_filename = match.groups()[0]
    sensing_start, sensing_stop = sorted(["%s-%s-%sT%s:%s:%s" % match.groups()[1:7],
                                          "%s-%s-%sT%s:%s:%s" % match.groups()[7:]])

    # get temporal_span
    temporal_span = getTemporalSpanInDays(sensing_stop, sensing_start)
    #get polarization from ifg xml
    try:
        fin = open(int_file, 'r')
        ifgxml = fin.read()
        fin.close()
        rslt = re.search(
            '<property name="polarization">[\s]*?<value>(HV|HH|VV|HH\+HV|VV\+VH)</value>', ifgxml, re.M)
        if rslt:
            polarization = rslt.group(1)
        else:
            logger.warn("Failed to get polarization from fine_interferogram.xml")
            polarization = 'ERR'
    except Exception as e:
        logger.warn("Failed to get polarization: %s" % traceback.format_exc())
        polarization = 'ERR'
    # load product and extract track-aligned bbox;
    # fall back to raster corner coords (not track aligned)
    try:
        prod = load_product(int_file)
        orb = get_orbit()
        bbox = get_aligned_bbox(prod, orb)
    except Exception as e:
        logger.warn("Failed to get aligned bbox: %s" % traceback.format_exc())
        logger.warn("Getting raster corner coords instead.")
        bbox = get_raster_corner_coords(vrt_file)

    #extract bperp and bpar
    cb_pkl = os.path.join(pickle_dir, "computeBaselines")
    with open(cb_pkl, 'rb') as f:
        catalog = pickle.load(f)
    bperp = catalog['baseline']['IW-{} Bperp at midrange for first common burst'.format(swath_num)]
    bpar = catalog['baseline']['IW-{} Bpar at midrange for first common burst'.format(swath_num)]
    ipf_version_master = catalog['master']['sensor']['processingsoftwareversion']
    ipf_version_slave = catalog['slave']['sensor']['processingsoftwareversion']

    # get mission char
    mis_char_master = MISSION_RE.search(master_mission).group(1)
    mis_char_slave = MISSION_RE.search(slave_mission).group(1)
    missions = [
        "Sentinel-1%s" % mis_char_master,
        "Sentinel-1%s" % mis_char_slave,
    ]

    # update metadata
    with open(json_file) as f:
        metadata = json.load(f)
    #update direction to ascending/descending
    if 'direction' in list(metadata.keys()):
        direct = metadata['direction']
        if direct == 'asc':
            direct = 'ascending'
        elif direct == 'dsc':
            direct = 'descending'
        metadata['direction'] = direct

    metadata.update({
        "tiles": True,
        "tile_layers": [ "amplitude", "interferogram" ],
        "archive_filename": archive_filename,
        "spacecraftName": missions,
        "platform": missions,
        "sensor": "SAR-C Sentinel1",
        "sensingStart": sensing_start,
        "sensingStop": sensing_stop,
        "temporal_span": temporal_span,
        "inputFile": "sentinel.ini",
        "product_type": "interferogram",
        "orbit_type": orbit_type,
        "polarization": polarization,
        "scene_count": int(scene_count),
        "imageCorners":{
            "minLat":minLat,
            "maxLat":maxLat,
            "minLon":minLon,
            "maxLon":maxLon
        },
        "bbox": bbox,
        "ogr_bbox": [[x, y] for y, x in bbox],
        "swath": [int(swath_num)],
        "perpendicularBaseline": bperp,
        "parallelBaseline": bpar,
        "version": [ipf_version_master, ipf_version_slave],
        "beamMode": "IW",
        "sha224sum": hashlib.sha224(str.encode(os.path.basename(json_file))).hexdigest(),
    })

    # remove outdated fields
    if 'verticalBaseline' in metadata: del metadata['verticalBaseline']
    if 'horizontalBaseline' in metadata: del metadata['horizontalBaseline']
    if 'totalBaseline' in metadata: del metadata['totalBaseline']

    # remove orbit; breaks index into elasticsearch because of it's format
    if 'orbit' in metadata: del metadata['orbit']

    # write final file
    with open(json_file, 'w') as f:
        json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    if len(sys.argv) != 11:
        raise SystemExit("usage: %s <orbit type used> <scene count> <swath num> <master_mission> <slave_mission> <pickle dir> <fine int file> <vrt file> <unw.geo.xml file> <output json file>" % sys.argv[0])
    orbit_type = sys.argv[1]
    scene_count = sys.argv[2]
    swath_num = sys.argv[3]
    master_mission = sys.argv[4]
    slave_mission = sys.argv[5]
    pickle_dir = sys.argv[6]
    int_file = sys.argv[7]
    vrt_file = sys.argv[8]
    xml_file = sys.argv[9]
    json_file = sys.argv[10]
    update_met_json(orbit_type, scene_count, swath_num, master_mission,
                    slave_mission, pickle_dir, int_file, vrt_file,
                    xml_file, json_file)
