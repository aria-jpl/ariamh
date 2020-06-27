#!/usr/bin/env python3 
import os, sys, re, requests, json, shutil, traceback, logging, hashlib, math
import math
import glob
from UrlUtils import UrlUtils
from subprocess import check_call, CalledProcessError
from datetime import datetime
import xml.etree.cElementTree as ET
from xml.etree.ElementTree import Element, SubElement
from zipfile import ZipFile
import isce_functions_alos2
import ifg_utils
from create_input_xml_alos2 import create_input_xml
from isceobj.Image.Image import Image
from lxml.etree import parse
import numpy as np
from utils.UrlUtils_standard_product import UrlUtils
from utils.imutils import get_image, get_size, crop_mask
from utils.time_utils import getTemporalSpanInDays
from osgeo import ogr, gdal

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_alos2_ifg.log')


BASE_PATH = os.path.dirname(__file__)


def main():

    id = "ALOS2-GUNW-D-R-153-scansar-20150412_20150301-182710-19999N_15000N-PP-0a17-v1_0"
    prod_dir = id
    uu = UrlUtils()

    # generate GDAL (ENVI) headers and move to product directory
    raster_prods = (
        #'insar/topophase.cor',
        #'insar/topophase.flat',
        #'insar/filt_topophase.flat',
        glob.glob('insar/filt_*-*_*rlks_*alks.unw')[0],
        glob.glob('insar/filt_*-*_*rlks_*alks.unw.conncomp')[0],
        glob.glob('insar/*-*_*rlks_*alks.cor')[0],
        glob.glob('insar/*-*_*rlks_*alks.los')[0],
        #'insar/los.rdr',
        #'insar/dem.crop',
    )
    for i in raster_prods:
        # radar-coded products
        os.system("isce2gis.py envi -i {}".format(i))
        gdal_xml = "{}.xml".format(i)
        gdal_hdr = "{}.hdr".format(i)
        gdal_vrt = "{}.vrt".format(i)

        # geo-coded products
        j = "{}.geo".format(i)
        if not os.path.exists(j): continue
        os.system("isce2gis.py envi -i {}".format(j))
        gdal_xml = "{}.xml".format(j)
        gdal_hdr = "{}.hdr".format(j)
        gdal_vrt = "{}.vrt".format(j)

    '''
    fine_int_xmls = []
    for swathnum in swath_list:
        fine_int_xmls.append("fine_interferogram/IW{}.xml".format(swathnum))
    '''

    # get water mask configuration
    wbd_url = uu.wbd_url
    wbd_user = uu.wbd_u
    wbd_pass = uu.wbd_p

    # get DEM bbox and add slop
    dem_S, dem_N, dem_W, dem_E = [15, 20, -101, -96] 
    dem_S = int(math.floor(dem_S))
    dem_N = int(math.ceil(dem_N))
    dem_W = int(math.floor(dem_W))
    dem_E = int(math.ceil(dem_E))
    dem_S = dem_S - 1 if dem_S > -89 else dem_S
    dem_N = dem_N + 1 if dem_N < 89 else dem_N
    dem_W = dem_W - 1 if dem_W > -179 else dem_W
    dem_E = dem_E + 1 if dem_E < 179 else dem_E

    # get water mask
    fp = open('wbdStitcher.xml','w')
    fp.write('<stitcher>\n')
    fp.write('    <component name="wbdstitcher">\n')
    fp.write('        <component name="wbd stitcher">\n')
    fp.write('            <property name="url">\n')
    fp.write('                <value>https://urlToRepository</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="action">\n')
    fp.write('                <value>stitch</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="directory">\n')
    fp.write('                <value>outputdir</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="bbox">\n')
    fp.write('                <value>[33,36,-119,-117]</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="keepWbds">\n')
    fp.write('                <value>False</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="noFilling">\n')
    fp.write('                <value>False</value>\n')
    fp.write('            </property>\n')
    fp.write('            <property name="nodata">\n')
    fp.write('                <value>-1</value>\n')
    fp.write('            </property>\n')
    fp.write('        </component>\n')
    fp.write('    </component>\n')
    fp.write('</stitcher>')
    fp.close()
    wbd_file = "wbdmask.wbd"
    wbd_cmd = [
        "{}/applications/wbdStitcher.py".format(os.environ['ISCE_HOME']), "wbdStitcher.xml",
        "wbdstitcher.wbdstitcher.bbox=[{},{},{},{}]".format(dem_S, dem_N, dem_W, dem_E),
        "wbdstitcher.wbdstitcher.outputfile={}".format(wbd_file),
        "wbdstitcher.wbdstitcher.url={}".format(wbd_url)
    ]
    wbd_cmd_line = " ".join(wbd_cmd)
    logger.info("Calling wbdStitcher.py: {}".format(wbd_cmd_line))
    try:
        check_call(wbd_cmd_line, shell=True)
    except Exception as e:
        logger.info(str(e))


    # get product image and size info
    #vrt_prod = get_image("insar/filt_topophase.unw.geo.xml")
    vrt_prod = get_image(glob.glob('insar/filt_*-*_*rlks_*alks.unw.geo.xml')[0])
    vrt_prod_size = get_size(vrt_prod)
    #flat_vrt_prod = get_image("insar/filt_topophase.flat.geo.xml")
    flat_vrt_prod = get_image(glob.glob('insar/*-*_*rlks_*alks.phsig.geo.xml')[0])
    flat_vrt_prod_size = get_size(flat_vrt_prod)

    # get water mask image and size info
    wbd_xml = "{}.xml".format(wbd_file)
    wmask = get_image(wbd_xml)
    wmask_size = get_size(wmask)

    # determine downsample ratio and dowsample water mask
    lon_rat = 1./(vrt_prod_size['lon']['delta']/wmask_size['lon']['delta'])*100
    lat_rat = 1./(vrt_prod_size['lat']['delta']/wmask_size['lat']['delta'])*100
    logger.info("lon_rat/lat_rat: {} {}".format(lon_rat, lat_rat))
    wbd_ds_file = "wbdmask_ds.wbd"
    wbd_ds_vrt = "wbdmask_ds.vrt"
    check_call("gdal_translate -of ENVI -outsize {}% {}% {} {}".format(lon_rat, lat_rat, wbd_file, wbd_ds_file), shell=True)
    check_call("gdal_translate -of VRT {} {}".format(wbd_ds_file, wbd_ds_vrt), shell=True)

    # update xml file for downsampled water mask
    wbd_ds_json = "{}.json".format(wbd_ds_file)
    check_call("gdalinfo -json {} > {}".format(wbd_ds_file, wbd_ds_json), shell=True)
    with open(wbd_ds_json) as f:
        info = json.load(f)
    with open(wbd_xml) as f:
        doc = parse(f)
    wbd_ds_xml = "{}.xml".format(wbd_ds_file)
    doc.xpath('.//component[@name="coordinate1"]/property[@name="delta"]/value')[0].text = str(info['geoTransform'][1])
    doc.xpath('.//component[@name="coordinate1"]/property[@name="size"]/value')[0].text = str(info['size'][0])
    doc.xpath('.//component[@name="coordinate2"]/property[@name="delta"]/value')[0].text = str(info['geoTransform'][5])
    doc.xpath('.//component[@name="coordinate2"]/property[@name="size"]/value')[0].text = str(info['size'][1])
    doc.xpath('.//property[@name="width"]/value')[0].text = str(info['size'][0])
    doc.xpath('.//property[@name="length"]/value')[0].text = str(info['size'][1])
    doc.xpath('.//property[@name="metadata_location"]/value')[0].text = wbd_ds_xml
    doc.xpath('.//property[@name="file_name"]/value')[0].text = wbd_ds_file
    for rm in doc.xpath('.//property[@name="extra_file_name"]'): rm.getparent().remove(rm)
    doc.write(wbd_ds_xml)

    # get downsampled water mask image and size info
    wmask_ds = get_image(wbd_ds_xml)
    wmask_ds_size = get_size(wmask_ds)

    logger.info("vrt_prod.filename: {}".format(vrt_prod.filename))
    logger.info("vrt_prod.bands: {}".format(vrt_prod.bands))
    logger.info("vrt_prod size: {}".format(vrt_prod_size))
    logger.info("wmask.filename: {}".format(wmask.filename))
    logger.info("wmask.bands: {}".format(wmask.bands))
    logger.info("wmask size: {}".format(wmask_size))
    logger.info("wmask_ds.filename: {}".format(wmask_ds.filename))
    logger.info("wmask_ds.bands: {}".format(wmask_ds.bands))
    logger.info("wmask_ds size: {}".format(wmask_ds_size))

    # crop the downsampled water mask
    wbd_cropped_file = "wbdmask_cropped.wbd"
    wmask_cropped = crop_mask(vrt_prod, wmask_ds, wbd_cropped_file)
    logger.info("wmask_cropped shape: {}".format(wmask_cropped.shape))

    # read in wrapped interferograma
    if "insar" not in flat_vrt_prod.filename:
        flat_vrt_prod.filename = os.path.join("insar", flat_vrt_prod.filename)
    flat_vrt_prod_shape = (flat_vrt_prod_size['lat']['size'], flat_vrt_prod_size['lon']['size'])
    flat_vrt_prod_im = np.memmap(flat_vrt_prod.filename,
                            dtype=flat_vrt_prod.toNumpyDataType(),
                            mode='c', shape=(flat_vrt_prod_size['lat']['size'], flat_vrt_prod_size['lon']['size']))
    phase = np.angle(flat_vrt_prod_im)
    phase[phase == 0] = -10
    phase[wmask_cropped == -1] = -10

    # mask out water from the product data
    if "insar" not in vrt_prod.filename:
        vrt_prod.filename = os.path.join("insar", vrt_prod.filename)
    vrt_prod_shape = (vrt_prod_size['lat']['size'], vrt_prod.bands, vrt_prod_size['lon']['size'])
    vrt_prod_im = np.memmap(vrt_prod.filename,
                            dtype=vrt_prod.toNumpyDataType(),
                            mode='c', shape=vrt_prod_shape)
    im1 = vrt_prod_im[:,:,:]
    for i in range(vrt_prod.bands):
        im1_tmp = im1[:,i,:]
        im1_tmp[wmask_cropped == -1] = 0

    # read in connected component mask
    #cc_vrt = "insar/filt_topophase.unw.conncomp.geo.vrt"
    cc_vrt = glob.glob('insar/filt_*2-*_*rlks_*alks.unw.conncomp.geo.vrt')[0]
    cc = gdal.Open(cc_vrt)
    cc_data = cc.ReadAsArray()
    cc = None
    logger.info("cc_data: {}".format(cc_data))
    logger.info("cc_data shape: {}".format(cc_data.shape))
    for i in range(vrt_prod.bands):
        im1_tmp = im1[:,i,:]
        im1_tmp[cc_data == 0] = 0
    phase[cc_data == 0] = -10

    # overwrite displacement with phase
    im1[:,1,:] = phase

    # create masked product image
    #masked_filt = "filt_topophase.masked.unw.geo"
    masked_filt =  glob.glob('insar/filt_*-*_*rlks_*alks_msk.unw')[0]
    #masked_filt_xml = "filt_topophase.masked.unw.geo.xml"
    masked_filt_xml = glob.glob('insar/filt_*-*_*rlks_*alks_msk.unw.xml')[0]
    tim = np.memmap(masked_filt, dtype=vrt_prod.toNumpyDataType(), mode='w+', shape=vrt_prod_shape)
    tim[:,:,:] = im1
    im  = Image()
    with open(glob.glob('insar/filt_*-*_*rlks_*alks.unw.geo.xml')[0]) as f:
        doc = parse(f)
    doc.xpath('.//property[@name="file_name"]/value')[0].text = masked_filt
    for rm in doc.xpath('.//property[@name="extra_file_name"]'): rm.getparent().remove(rm)
    doc.write(masked_filt_xml)
    im.load(masked_filt_xml)
    latstart = vrt_prod_size['lat']['val']
    lonstart = vrt_prod_size['lon']['val']
    latsize = vrt_prod_size['lat']['size']
    lonsize = vrt_prod_size['lon']['size']
    latdelta = vrt_prod_size['lat']['delta']
    londelta = vrt_prod_size['lon']['delta']
    im.coord2.coordStart = latstart
    im.coord2.coordSize = latsize
    im.coord2.coordDelta = latdelta
    im.coord2.coordEnd = latstart + latsize*latdelta
    im.coord1.coordStart = lonstart
    im.coord1.coordSize = lonsize
    im.coord1.coordDelta = londelta
    im.coord1.coordEnd = lonstart + lonsize*londelta
    im.filename = masked_filt
    im.renderHdr()

    # mask out nodata values
    vrt_prod_file = glob.glob('insar/filt_*-*_*rlks_*alks_msk.unw.vrt')[0]
    vrt_prod_file_amp = "filt_topophase.masked_nodata.unw.amp.geo.vrt"
    vrt_prod_file_dis = "filt_topophase.masked_nodata.unw.dis.geo.vrt"
    check_call("gdal_translate -of VRT -b 1 -a_nodata 0 {} {}".format(vrt_prod_file, vrt_prod_file_amp), shell=True)
    check_call("gdal_translate -of VRT -b 2 -a_nodata -10 {} {}".format(vrt_prod_file, vrt_prod_file_dis), shell=True)
    
    '''
    # get band statistics
    amp_data = gdal.Open(vrt_prod_file_amp, gdal.GA_ReadOnly)
    #band_stats_amp = amp_data.GetRasterBand(1).GetStatistics(0, 1)
    dis_data = gdal.Open(vrt_prod_file_dis, gdal.GA_ReadOnly)
    band_stats_dis = dis_data.GetRasterBand(1).GetStatistics(0, 1)
    #logger.info("amplitude band stats: {}".format(band_stats_amp))
    logger.info("displacment band stats: {}".format(band_stats_dis))
    '''

    # create interferogram tile layer
    tiles_dir = "{}/tiles".format(prod_dir)
    tiler_cmd_path = os.path.abspath(os.path.join(BASE_PATH, '..', '..', 'map_tiler'))
    dis_layer = "interferogram"
    tiler_cmd_tmpl = "{}/create_tiles.py {} {}/{} -b 1 -m hsv --clim_min {} --clim_max {} --nodata 0"
    check_call(tiler_cmd_tmpl.format(tiler_cmd_path, vrt_prod_file_dis, tiles_dir, dis_layer, -3.14, 3.14), shell=True)

    # create amplitude tile layer
    amp_layer = "amplitude"
    tiler_cmd_tmpl = "{}/create_tiles.py {} {}/{} -b 1 -m gray --clim_min {} --clim_max {} --nodata 0"
    #check_call(tiler_cmd_tmpl.format(tiler_cmd_path, vrt_prod_file_amp, tiles_dir, amp_layer, band_stats_amp[0], band_stats_amp[1]), shell=True)

    # create browse images
    tif_file_dis = "{}.tif".format(vrt_prod_file_dis)
    check_call("gdal_translate -of png -r average -tr 0.00416666667 0.00416666667 {} {}/{}.interferogram.browse_coarse.png".format(tif_file_dis, prod_dir, id), shell=True)
    check_call("gdal_translate -of png {} {}/{}.interferogram.browse_full.png".format(tif_file_dis, prod_dir, id), shell=True)
    tif_file_amp = "{}.tif".format(vrt_prod_file_amp)
    #check_call("gdal_translate -of png -r average -tr 0.00416666667 0.00416666667 {} {}/{}.amplitude.browse_coarse.png".format(tif_file_amp, prod_dir, id), shell=True)
    #check_call("gdal_translate -of png {} {}/{}.amplitude.browse_full.png".format(tif_file_amp, prod_dir, id), shell=True)
    for i in glob("{}/{}.*.browse*.aux.xml".format(prod_dir, id)): os.unlink(i)


if __name__ == '__main__':
    main()
