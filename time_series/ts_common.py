import os
import h5py
import json
import re
import datetime
import numpy as np
import scipy.spatial
from osgeo import gdal, ogr

def get_geom(vrt_file):
    """Return geocoded coordinates of radar pixels as a GDAL geom."""

    # extract geo-coded corner coordinates
    ds = gdal.Open(vrt_file)
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    lon_arr = [0, cols-1]
    lat_arr = [0, rows-1]
    lons = []
    lats = []
    #logger.info("lon_arr: %s" % lon_arr)
    #logger.info("lat_arr: %s" % lat_arr)
    for py in lat_arr:
        lats.append(gt[3] + (py * gt[5]))
    for px in lon_arr:
        lons.append(gt[0] + (px * gt[1]))
    return ogr.CreateGeometryFromJson(json.dumps({
        'type': 'Polygon',
        'coordinates': [[
            [ lons[0], lats[0] ],
            [ lons[0], lats[1] ],
            [ lons[1], lats[1] ],
            [ lons[1], lats[0] ],
            [ lons[0], lats[0] ],
        ]]
    }))


def get_envelope(product_dirs):
    """Return overlap bbox of all interferograms."""

    # Create a geometry collection
    geom_col =  ogr.Geometry(ogr.wkbGeometryCollection)
    for prod in product_dirs:
        unw_vrt = os.path.join(prod, "merged", "filt_topophase.unw.geo.vrt")
        geom = get_geom(unw_vrt)
        geom_col.AddGeometry(geom)
        #logger.info("-" * 80)
        #logger.info("{}: {}".format(prod, geom.GetEnvelope()))
        #logger.info("envelope: {}".format(geom_col.GetEnvelope()))
    return geom_col.GetEnvelope()


def get_bounding_polygon(path):
    '''
    Get the minimum bounding region
    @param path - path to h5 file from which to read TS data
    '''
    fle = h5py.File("./Stack/NSBAS-PARAMS.h5", "r")
    #Read out the first data frame, lats vector and lons vector.
    data = fle["rawts"][0]
    lons = fle["lon"]
    lats = fle["lat"]
    #Create a grid of lon, lat pairs
    coords = np.dstack(np.meshgrid(lons,lats))
    #Calculate any point in the data that is not NaN, and grab the coordinates 
    inx = ~np.isnan(data)
    points = coords[inx]
    #Calculate the convex-hull of the data points.  This will be a mimimum
    #bounding convex-polygon.
    hull = scipy.spatial.ConvexHull(points)
    #Harvest the points and make it a loop
    pts = [list(pt) for pt in hull.points[hull.vertices]]
    pts.append(pts[0])
    return pts

def get_bperp(catalog):
    '''
    Return perpendicular baseline.
    @param catalog - catalog object to search for Bperp keys
    '''
    for i in catalog['baseline']:
        if re.search(r'Bperp at midrange for first common burst', i):
            return catalog['baseline'][i]
    raise RuntimeError("Failed to find perpendicular baseline.")
def get_version():
    """Get dataset version."""

    DS_VERS_CFG = os.path.normpath(os.path.join(
                          os.path.dirname(os.path.abspath(__file__)),
                          '..', 'conf', 'dataset_versions.json'))
    with open(DS_VERS_CFG) as f:
        ds_vers = json.load(f)
    return ds_vers['TS']
def write_dataset_json(prod_dir,id,region,starttime,endtime):
    '''
    Write a dataset JSON file for TS
    @param prod_dir: product directory
    @param id: id of product
    @param region: region to GEO-JSONize
    @param starttime: starttime of the data
    @param endtime: endtime of the data
    '''
    met = {
        'creation_timestamp': "%sZ" % datetime.datetime.utcnow().isoformat(),
        'version': get_version(),
        'label': id,
        'location': {
            "type": "Polygon",
            "coordinates": [region]
        },
        "starttime":starttime,
        "endtime":endtime
    }
    dataset_file = os.path.join(prod_dir, "{}.dataset.json".format(id))
    with open(dataset_file, 'w') as f:
        json.dump(met, f, indent=2)
