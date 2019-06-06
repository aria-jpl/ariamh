#!/usr/bin/env python3
"""
Return absolute min, absolute max, min percentile, and max percentile
values of data in a raster band.
"""
import os, sys, argparse, logging, traceback
import numpy as np
from osgeo import gdal
from gdalconst import GA_ReadOnly


gdal.UseExceptions() # make GDAL raise python exceptions


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('get_clims')


def get_clims(raster, band, clim_min_pct=None, clim_max_pct=None, nodata=None):
    """Get data absolute min/max values as well as min/max percentile values
       for a given GDAL-recognized file format for a particular band."""

    # load raster
    gd = gdal.Open(raster, GA_ReadOnly)

    # get number of bands
    bands = gd.RasterCount

    # process the raster
    b = gd.GetRasterBand(band)
    d = b.ReadAsArray()
    logger.info("band data: {}".format(d))
    # fetch max and min
    #min = band.GetMinimum()
    #max = band.GetMaximum()
    if nodata is not None:
        d = np.ma.masked_equal(d, nodata)
    min = np.amin(d)
    max = np.amax(d)
    min_pct = np.percentile(d, clim_min_pct) if clim_min_pct is not None else None
    max_pct = np.percentile(d, clim_max_pct) if clim_max_pct is not None else None
    
    logger.info("band {} absolute min/max: {} {}".format(band, min, max))
    logger.info("band {} {}/{} percentiles: {} {}".format(band, clim_min_pct,
                                                          clim_max_pct, min_pct,
                                                          max_pct))
    gd = None

    return min, max, min_pct, max_pct


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raster", help="input raster file (any GDAL-recognized file format)")
    parser.add_argument("-b", "--band", dest="band", type=int, default=1,
                        help="raster band")
    parser.add_argument("--clim_min_pct", dest="clim_min_pct", type=float,
                        default=10, help="color limit min percent")
    parser.add_argument("--clim_max_pct", dest="clim_max_pct", type=float,
                        default=90, help="color limit max percent")
    args = parser.parse_args()
    min, max, min_pct, max_pct = get_clims(args.raster, args.band,
                                           args.clim_min_pct, args.clim_max_pct)
