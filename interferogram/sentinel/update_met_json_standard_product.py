#!/usr/bin/env python3
import ast, os, sys, json, re, math, logging, traceback, pickle, hashlib
from lxml.etree import parse
from osgeo import gdal, ogr, osr
import numpy as np
import isce
from iscesys.Component.ProductManager import ProductManager as PM
from isceobj.Orbit.Orbit import Orbit

from utils.time_utils import getTemporalSpanInDays


gdal.UseExceptions() # make GDAL raise python exceptions


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('update_met_json')


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

def change_direction(coords):
    cord_area= get_area(coords)
    if not get_area(coords) > 0: #reverse order if not clockwise
        print("update_met_json, reversing the coords")
        coords = coords[::-1]
    return coords


def get_loc(box):
    """Return GeoJSON bbox."""
    bbox = np.array(box).astype(np.float)
    coords = [
        [ bbox[0,1], bbox[0,0] ],
        [ bbox[1,1], bbox[1,0] ],
        [ bbox[2,1], bbox[2,0] ],
        [ bbox[3,1], bbox[3,0] ],
        [ bbox[0,1], bbox[0,0] ],
    ]
    return {
        "type": "Polygon",
        "coordinates":  [coords] 
    }

def get_env_box(env):

    #print("get_env_box env " %env)
    bbox = [
        [ env[3], env[0] ],
        [ env[3], env[1] ],
        [ env[2], env[1] ],
        [ env[2], env[0] ],
    ]
    print("get_env_box box : %s" %bbox)
    return bbox


def get_union_geom(bbox_list):
    geom_union = None
    for bbox in bbox_list:
        loc = get_loc(bbox)
        geom = ogr.CreateGeometryFromJson(json.dumps(loc))
        print("get_union_geom : geom : %s" %get_union_geom)
        if geom_union is None:
            geom_union = geom
        else:
            geom_union = geom_union.Union(geom)
    print("geom_union_type : %s" %type(geom_union)) 
    return geom_union

def update_met_json(orbit_type, scene_count, swath_num, master_mission,
                    slave_mission, pickle_dir, int_files, vrt_file, 
                    xml_file, json_file, sensing_start, sensing_stop,
                    archive_filename):
    """Write product metadata json."""
    print("update_met_json : swath_num : %s type : %s" %(swath_num, type(swath_num)))
    print("update_met_json : int_files : %s : %s" %(int_files, type(int_files)))
    print("update_met_json : xml file : %s" %(int_files))
    print("update_met_json : sensing_start: %s  sensing_stop : %s" %(sensing_start, sensing_stop))

    bboxes = []
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

    # get temporal_span
    temporal_span = getTemporalSpanInDays(sensing_stop, sensing_start)
    
    #get polarization from ifg xml
    int_file = int_files[0]
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
    
    bbox = None
    for int_file in int_files:
        try:
            prod = load_product(int_file)
            orb = get_orbit()
            bbox_swath = get_aligned_bbox(prod, orb)
        except Exception as e:
            logger.warn("Failed to get aligned bbox: %s" % traceback.format_exc())
            logger.warn("Getting raster corner coords instead.")
            bbox_swath = get_raster_corner_coords(vrt_file)
        print("bbox_swath : %s" %bbox_swath)
        bboxes.append(bbox_swath)

    geom_union = get_union_geom(bboxes)
    bbox = json.loads(geom_union.ExportToJson())["coordinates"][0]
    print("First Union Bbox : %s " %bbox)
    bbox = get_env_box(geom_union.GetEnvelope())
    print("Get Envelop :Final bbox : %s" %bbox)    
    
    bbox=change_direction(bbox)

    ogr_bbox = [[x, y] for y, x in bbox]

    ogr_bbox = change_direction(ogr_bbox)
    #extract bperp and bpar
    cb_pkl = os.path.join(pickle_dir, "computeBaselines")
    with open(cb_pkl, 'rb') as f:
        catalog = pickle.load(f)
    bperp = catalog['baseline']['IW-{} Bperp at midrange for first common burst'.format(2)]
    bpar = catalog['baseline']['IW-{} Bpar at midrange for first common burst'.format(2)]
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
    if 'direction' in metadata.keys():
        direct = metadata['direction']
        if direct == 'asc':
            direct = 'ascending'
        elif direct == 'dsc':
            direct = 'descending'
        metadata['direction'] = direct

    metadata.update({
        "tiles": True,
        #"tile_layers": [ "amplitude", "interferogram" ],
        "tile_layers": [ "interferogram" ],
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
        "ogr_bbox": ogr_bbox,
        #"swath": [int(swath_num)],
	#"swath": swath_num,
        "perpendicularBaseline": bperp,
        "parallelBaseline": bpar,
        "ipf_version": [ipf_version_master, ipf_version_slave],
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
    print("update met arg count : %s" %len(sys.argv))
    if len(sys.argv) != 14:
        raise SystemExit("usage: %s <orbit type used> <scene count> <swath num> <master_mission> <slave_mission> <pickle dir> <fine int file> <vrt file> <unw.geo.xml file> <output json file> <sensing start> <sensing stop> <archive filename>" % sys.argv[0])
    orbit_type = sys.argv[1]
    print("orbit_type :%s"%orbit_type)
    scene_count = sys.argv[2]
    print("scene_count : %s"%scene_count)
    swath_num = ast.literal_eval(sys.argv[3])
    print("swath_num : %s "%swath_num)
    master_mission = sys.argv[4]
    print("master_mission : %s "%master_mission)
    slave_mission = sys.argv[5]
    print("slave_mission : %s"%slave_mission)
    pickle_dir = sys.argv[6]
    print("pickle_dir : %s"%pickle_dir)
    int_files = ast.literal_eval(sys.argv[7])
    print("int_files %s"%int_files)
    vrt_file = sys.argv[8]
    print("vrt_file : %s"%vrt_file)
    xml_file = sys.argv[9]
    print("xml_file : %s"%xml_file)
    json_file = sys.argv[10]
    print("json_file : %s"%json_file)
    sensing_start = sys.argv[11]
    print("sensing_start : %s"%sensing_start)
    sensing_stop = sys.argv[12]
    print("sensing_stop : %s"%sensing_stop)
    archive_filename = sys.argv[13]
    print("archive_filename : %s"%archive_filename)
    update_met_json(orbit_type, scene_count, swath_num, master_mission,
                    slave_mission, pickle_dir, int_files, vrt_file,
                    xml_file, json_file, sensing_start, sensing_stop,
                    archive_filename)
