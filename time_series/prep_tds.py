#!/usr/bin/env python3
"""
Add lat, lon and time variables to GIAnT time-series product to
enable WMS capability when hosted on TDS (THREDDS Data Server).
"""

import os, sys, traceback, logging, argparse, h5py
import numpy as np
from datetime import datetime
from osgeo import gdal


gdal.UseExceptions() # make GDAL raise python exceptions


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('prep_tds')


def get_geocoded_coords(vrt_file):
    """Return geocoded coordinates of radar pixels."""

    # extract geo-coded corner coordinates
    ds = gdal.Open(vrt_file)
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    lon_arr = list(range(0, cols))
    lat_arr = list(range(0, rows))
    lons = np.empty((cols,))
    lats = np.empty((rows,))
    #logger.info("lon_arr: %s" % lon_arr)
    #logger.info("lat_arr: %s" % lat_arr)
    for py in lat_arr:
        lats[py] = gt[3] + (py * gt[5])
    for px in lon_arr:
        lons[px] = gt[0] + (px * gt[1])
    return lats, lons


def prep_tds(aligned_vrt, h5_file):
    """Add lat, lon, and time info for TDS compatibility."""

    #logger.info(aligned_vrt)
    #logger.info(h5_file)

    # get geocoded coordinates
    lats, lons = get_geocoded_coords(aligned_vrt)
    #logger.info(lats)
    #logger.info(lats.shape)
    #logger.info(lons)
    #logger.info(lons.shape)

    #Open a file for append
    h5f = h5py.File(h5_file, "r+")

    #Calculate times from ordinals
    dates = h5f.get("dates")
    times = [int(datetime.fromordinal(int(item)).strftime("%s")) for item in dates]

    #Create time, lat, and lon dataset
    time = h5f.create_dataset("time",dates.shape, "d")
    time[:] = times
    lat = h5f.create_dataset("lat", lats.shape, "d")
    lat[:] = lats
    lon = h5f.create_dataset("lon", lons.shape, "d")
    lon[:] = lons

    #Create new dimension vars
    dims = {}
    time.attrs.create("axis", np.string_("T"))
    time.attrs.create("units", np.string_("seconds since 1970-01-01 00:00:00 +0000"))
    time.attrs.create("standard_name", np.string_("time"))
    lat.attrs.create("help", np.string_("Latitude array"))
    lon.attrs.create("help", np.string_("Longitude array"))

    #Attach the new time dimension to the rawts and recons as scales
    #In addition, attach lat and lon as scales
    for dset_name in ["rawts", "recons", "error"]:
        dset = h5f.get(dset_name)
        if dset is None: continue
        dset.dims.create_scale(time, "time")
        dset.dims[0].attach_scale(time)
        dset.dims.create_scale(lat, "lat")
        dset.dims[1].attach_scale(lat)
        dset.dims.create_scale(lon, "lon")
        dset.dims[2].attach_scale(lon)

        # add units attribute
        dset.attrs.create("units", np.string_("mm"))

    #Close file
    h5f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("aligned_vrt",
                        help="example VRT image aligned to region of interest" +
                             " used in producing the GIAnT product",
                        default="aligned.cor.vrt")
    parser.add_argument("h5_file", nargs='?',
                        help="HDF5 GIAnT product file (default: NSBAS-PARAMS.h5)",
                        default="NSBAS-PARAMS.h5")
    args = parser.parse_args()
    try: prep_tds(args.aligned_vrt, args.h5_file)
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("{}\n".format(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("{}\n".format(traceback.format_exc()))
        raise
