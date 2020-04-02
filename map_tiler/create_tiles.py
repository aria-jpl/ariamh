#!/usr/bin/env python3
"""
Map tiler PGE wrapper to generate map tiles following the 
OSGeo Tile Map Service Specification.
"""
from __future__ import absolute_import

from builtins import str
import os, sys, traceback, logging, argparse
from subprocess import check_call

from get_clims import get_clims


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_tiles')


BASE_PATH = os.path.dirname(__file__)


def call_noerr(cmd):
    """Run command and warn if exit status is not 0."""

    try: check_call(cmd, shell=True)
    except Exception as e:
        logger.warn("Got exception running {}: {}".format(cmd, str(e)))
        logger.warn("Traceback: {}".format(traceback.format_exc()))


def create_tiles(raster, output_dir, band=1, cmap='jet', clim_min=None,
                 clim_max=None, clim_min_pct=None, clim_max_pct=None,
                 zoom=[0, 8], nodata=None):
    """Generate map tiles following the OSGeo Tile Map Service Specification."""

    # check mutually exclusive args
    if clim_min is not None and clim_min_pct is not None:
        raise RuntimeError
    if clim_max is not None and clim_max_pct is not None:
        raise RuntimeError

    # get clim
    min, max, min_pct, max_pct = get_clims(raster, band,
                                           clim_min_pct if clim_min_pct is not None else 20,
                                           clim_max_pct if clim_max_pct is not None else 80,
                                           nodata)

    # overwrite if options not specified
    if clim_min is not None: min = clim_min
    if clim_max is not None: max = clim_max
    if clim_min_pct is not None: min = min_pct
    if clim_max_pct is not None: max = max_pct

    # convert to geotiff
    logger.info("Generating GeoTIFF.")
    tif_file = "{}.tif".format(os.path.basename(raster))
    if os.path.exists(tif_file): os.unlink(tif_file)
    cmd = "isce2geotiff.py -i {} -o {} -c {:f} {:f} -b {} -m {}"
    check_call(cmd.format(raster, tif_file, min, max, band-1, cmap), shell=True)

    # create tiles from geotiff
    logger.info("Generating tiles.")
    zoom_i = zoom[0]
    zoom_f = zoom[1]
    while zoom_f > zoom_i:
        try:
            if nodata is None:
                cmd = "gdal2tiles.py -z {}-{} -p mercator {} {}".format(zoom_i, zoom_f, tif_file, output_dir)
            else:
                cmd = "gdal2tiles.py -z {}-{} -p mercator -a {} {} {}".format(zoom_i, zoom_f, nodata, tif_file, output_dir)
            logger.info("cmd: %s" % cmd)
            check_call(cmd, shell=True)
            break
        except Exception as e:
            logger.warn("Got exception running {}: {}".format(cmd, str(e)))
            logger.warn("Traceback: {}".format(traceback.format_exc()))
            zoom_f -= 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raster", help="input raster file (any GDAL-recognized file format)")
    parser.add_argument("output_dir", help="output directory")
    parser.add_argument("-b", "--band", dest="band", type=int,
                        default=1, help="raster band")
    parser.add_argument("-m", "--cmap", dest="cmap", type=str, 
                        default='jet', help="matplotlib colormap")
    parser.add_argument("--clim_min", dest="clim_min", type=float, 
                        default=None, help="color limit min value")
    parser.add_argument("--clim_max", dest="clim_max", type=float,
                        default=None, help="color limit max value")
    parser.add_argument("--clim_min_pct", dest="clim_min_pct", type=float,
                        default=None, help="color limit min percent")
    parser.add_argument("--clim_max_pct", dest="clim_max_pct", type=float,
                        default=None, help="color limit max percent")
    parser.add_argument("-z", "--zoom", dest='zoom', type=int, nargs=2,
                        default=[0, 8], help='zoom level range to create tiles for')
    parser.add_argument("--nodata", dest="nodata", type=float,
                        default=None, help="nodata value")
    args = parser.parse_args()
    status = create_tiles(args.raster, args.output_dir, args.band, args.cmap,
                          args.clim_min, args.clim_max, args.clim_min_pct,
                          args.clim_max_pct, args.zoom, args.nodata)
