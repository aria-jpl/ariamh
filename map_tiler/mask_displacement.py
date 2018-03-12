#!/usr/bin/env python3
"""
Mask displacement values where amplitude is less than some threshold.
"""
import os, sys, argparse, logging, traceback
import numpy as np
from osgeo import gdal, ogr, osr
from gdalconst import GA_ReadOnly


gdal.UseExceptions()


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('mask_displacement')


def translate(in_file, out_file, amp_threshold=300, no_data_value=0.):
    """Use amplitude to mask displacement."""

    # read in raster bands
    in_ds = gdal.Open(in_file, GA_ReadOnly)
    gt = in_ds.GetGeoTransform()
    cols = in_ds.RasterXSize
    rows = in_ds.RasterYSize
    amp_band = in_ds.GetRasterBand(1)
    #print("amp_band no data value: {}".format(amp_band.GetNoDataValue()))
    amp = amp_band.ReadAsArray()
    dis_band = in_ds.GetRasterBand(2)
    #print("dis_band no data value: {}".format(dis_band.GetNoDataValue()))
    dis = dis_band.ReadAsArray()
    #print("col: {}, row: {}, amp: {}, dis: {}".format(1488, 2832, amp[1488,2832], dis[1488,2832]))

    # create masked array
    dis_masked = np.ma.masked_array(dis, amp < amp_threshold)

    # create output raster
    out_ds = gdal.GetDriverByName('GTiff').Create(out_file, cols, rows, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(gt)
    dis_band_out = out_ds.GetRasterBand(1)
    dis_band_out.SetNoDataValue(no_data_value)
    dis_band_out.WriteArray(dis_masked.filled(no_data_value))
    out_srs = osr.SpatialReference()
    out_srs.ImportFromWkt(in_ds.GetProjectionRef())
    out_ds.SetProjection(out_srs.ExportToWkt())
    dis_band_out.FlushCache()
    in_ds = None
    out_ds = None 


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("in_file", help="input raster file (any GDAL-recognized file format)")
    parser.add_argument("out_file", help="output GeoTIFF raster file")
    parser.add_argument("-a", "--amp_threshold", dest="amp_threshold",
                        type=int, default=300, help="amplitude threshold")
    parser.add_argument("-n", "--no_data_value", dest="no_data_value",
                        type=int, default=0, help="no data value")
    args = parser.parse_args()
    translate(args.in_file, args.out_file, args.amp_threshold, args.no_data_value)
