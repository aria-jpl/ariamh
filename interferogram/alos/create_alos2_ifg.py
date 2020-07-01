#!/usr/bin/env python3 
import os, sys, re, requests, json, shutil, traceback, logging, hashlib, math
import math
from glob import glob
from UrlUtils import UrlUtils
from subprocess import check_call, CalledProcessError
from datetime import datetime
import xml.etree.cElementTree as ET
from xml.etree.ElementTree import Element, SubElement
from zipfile import ZipFile
import isce_functions_alos2
import ifg_utils
from create_input_xml_alos2 import create_input_xml

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_alos2_ifg.log')


BASE_PATH = os.path.dirname(__file__)

IMG_RE=r'IMG-(\w{2})-ALOS(\d{6})(\d{4})-*'
IFG_ID_ALOS2_TMPL = "ALOS2-INSARZD-{}-{}-{}-{}"
IFG_ID_ALOS2_TMPL = "ALOS2-GUNW-{}-{}-{:03d}-scansar-{}_{}-{}-{}-PP-{}-{}"
SLC_FILTERS = ['IMG-HH', 'LED', 'TRL']

def main():


    ''' Run the install '''
    wd = os.getcwd()
    ifg_md = {}    
    
    
    ''' Get the informations from _context file '''
    ctx_file = os.path.abspath('_context.json')
    if not os.path.exists(ctx_file):
        raise RuntimeError("Failed to find _context.json.")
    with open(ctx_file) as f:
        ctx = json.load(f)

    # save cwd (working directory)
    complete_start_time=datetime.now()
    logger.info("Alos2 start Time : {}".format(complete_start_time))

    dem_type = ctx['dem_type']
    reference_slc = ctx['reference_product']
    secondary_slc = ctx['secondary_product']
    
    ifg_type = ctx["ifg_type"]
    azimuth_looks = ctx["azimuth_looks"]
    range_looks = ctx["range_looks"]
    burst_overlap = ctx["burst_overlap"]
    filter_strength = ctx["filter_strength"]

    ref_slc_filelist = ifg_utils.get_zip_contents(reference_slc)
    sec_slc_filelist = ifg_utils.get_zip_contents(secondary_slc)

    #Unzip the slc files
    slcs = {"reference" : "{}".format(reference_slc), "secondary" : "{}".format(secondary_slc)}
    ifg_utils.unzip_slcs(slcs, SLC_FILTERS)

    ifg_hash = ifg_utils.get_ifg_hash([reference_slc], [secondary_slc])

    ifg_md['full_idc_hash'] = ifg_hash
    ifg_md['dem_type'] = dem_type
    ifg_md['reference_slc'] = reference_slc
    ifg_md['secondary_slc'] = secondary_slc
    ifg_md["interferogram_type"] = ifg_type
    ifg_md["azimuth_looks"] = int(azimuth_looks)
    ifg_md["range_looks"] = int(range_looks)
    ifg_md["burst_overlap"] = float(burst_overlap)
    ifg_md["filter_strength"] = float(filter_strength)

    version = ifg_utils.get_version("ALOS2_IFG")
    if not version:
        version = "v1.0"
   
    start_subswath = 1
    end_subswath = 5

    ifg_md["start_subswath"] = start_subswath
    ifg_md["end_subswath"] = end_subswath
    
    ref_data_dir = os.path.join(wd, "reference")
    sec_data_dir = os.path.join(wd, "secondary")

    alos2_script_path = os.environ['ISCE_HOME']
    print("alos2_script_path : {}".format(alos2_script_path))

    ifg_utils.change_dir(wd)

    ''' Extrach SLC Metadata '''
    ref_md = isce_functions_alos2.create_alos2_md_json(ref_data_dir)
    sec_md = isce_functions_alos2.create_alos2_md_json(sec_data_dir)

    '''
    ref_md_json = "ref_alos2_md.json"
    with open(ref_md_json, "w") as f:
        json.dump(ref_md, f, indent=2)
        f.close()

    sec_md_json = "sec_alos2_md.json"
    with open(sec_md_json, "w") as f:
        json.dump(sec_md, f, indent=2)
        f.close()

    isce_functions_alos2.create_alos2_md_isce(ref_data_dir, "ref_alos2_md.json")
    isce_functions_alos2.create_alos2_md_isce(sec_data_dir, "sec_alos2_md.json")
    with open("ref_alos2_md.json") as f:
        ref_md = json.load(f)
    with open("sec_alos2_md.json") as f:
        sec_md = json.load(f)
    '''

    
    
    ref_md['location'] = ref_md.pop('geometry')
    sec_md['location'] = sec_md.pop('geometry')

    
    
    sat_direction = "D"
    direction = ref_md["flight_direction"]
    if direction.lower() == 'asc':
        sat_direction = "A"

    pointing_direction = "L"
    if ref_md['pointing_direction'].lower() == 'right':
        pointing_direction = "R"

    dt_string = datetime.now().strftime("%d%m%YT%H%M%S")
    ifg_hash = ifg_hash[0:4]


    '''
    #Check if ifg_already exists
    #id = IFG_ID_ALOS2_TMPL.format(sat_direction, dt_string, ifg_hash, version.replace('.', '_') )

    #id = "ALOS2-INSARZD-D-18042020T154753-4be9-v1_0"
    if ifg_utils.check_ifg_status(id, "grq"):
        print("{} Already Exists. Exiting ....".format(id))
        exit(0)
    '''

    ifg_md['track'] = ref_md['track'] 

    ifg_md['satelite_direction'] = direction
    ref_orbit = ref_md["absolute_orbit"]
    sec_orbit = sec_md["absolute_orbit"]
    ifg_md["orbit"] = list(set([ref_orbit, sec_orbit]))

    ref_frame = int(ref_md["frame"])
    sec_frame = int(sec_md["frame"])

    if ref_frame != sec_frame:
        print("Reference Frame : {} is NOT same as Secondery Frame : {}".format(ref_frame, sec_frame))
        #raise Exception("Reference Frame : {} is NOT same as Secondery Frame : {}".format(ref_frame, sec_frame))

    ifg_md["frame"] = "{}".format(ref_frame)
    
    print("ref_md['location'] : {}".format(ref_md['location']))

    ref_bbox = ref_md['location']['coordinates'][0]
    sec_bbox = sec_md['location']['coordinates'][0]
    union_geojson = ifg_utils.get_union_geometry([ref_md['location'], sec_md['location']])
    ifg_md["union_geojson"] = union_geojson
    print(union_geojson)

    SNWE, snwe_arr = ifg_utils.get_SNWE_complete_bbox(ref_bbox, sec_bbox)
    ifg_md["SNWE"] = SNWE
    logging.info("snwe_arr : {}".format(snwe_arr))
    logging.info("SNWE : {}".format(SNWE))
    
    preprocess_dem_file, geocode_dem_file, preprocess_dem_xml, geocode_dem_xml = ifg_utils.download_dem(SNWE)
   

    ref_pol, ref_frame_arr = ifg_utils.get_pol_frame_info(ref_slc_filelist)
    sec_pol, sec_frame_arr = ifg_utils.get_pol_frame_info(sec_slc_filelist)

    if ref_pol != sec_pol:
        raise Exception("REF Pol : {} is different than SEC Pol : {}".format(ref_pol, sec_pol))

    if set(ref_frame_arr) != set(sec_frame_arr):
        raise Exception("REF Frame : {} is different than SEC Frame : {}".format(ref_frame_arr, sec_frame_arr))
    '''
    Logic for Fram datas
    '''

    ifg_md["polarization"] = ref_pol
    ifg_md['sensing_start'] = ref_md["sensing_start"], 
    acq_center_time = ifg_utils.get_center_time(ref_md['sensing_start'] , ref_md['sensing_stop'] )
    ref_dt = ref_md["sensing_start"].split('T')[0].replace('-', '')
    sec_dt = sec_md["sensing_start"].split('T')[0].replace('-', '')

    xml_file = "alos2app_{}.xml".format(ifg_type)
    tmpl_file = "{}.tmpl".format(xml_file)

    tmpl_file = os.path.join(BASE_PATH, tmpl_file)
    print(tmpl_file)
    create_input_xml(tmpl_file, xml_file,
                     str(ref_data_dir), str(sec_data_dir),
                     str(preprocess_dem_file), str(geocode_dem_file), start_subswath, end_subswath, burst_overlap,
                     str(ref_pol), str(ref_frame_arr), str(sec_pol), str(sec_frame_arr), snwe_arr)


    alos2_start_time=datetime.now()
    logger.info("ALOS2 Start Time : {}".format(alos2_start_time)) 

    ifg_utils.change_dir(wd)
    cmd = ["python3", "{}/applications/alos2App.py".format(os.environ['ISCE_HOME']), "{}".format(xml_file), "{}".format("--steps")]
    ifg_utils.run_command(cmd)
    
    '''
    cmd = ["python3", "{}/applications/ion.py".format(os.environ['ISCE_HOME']),  "{}".format(xml_file)]
    ifg_utils.run_command(cmd)

    cmd = ["python3", "{}/applications/alos2App.py".format(os.environ['ISCE_HOME']), "-i", "{}".format(xml_file), "-s", "filter"]
    ifg_utils.run_command(cmd)
    '''
    ifg_md['sensing_stop'] = "%sZ" % datetime.utcnow().isoformat('T')

    ifg_utils.change_dir(wd)
    isce_functions_alos2.create_alos2_md_file("reference", "ref_alos2_md.json")
    isce_functions_alos2.create_alos2_md_file("secondary", "sec_alos2_md.json")

    insar_dir = os.path.join(wd, "insar")
    #vrt_file = "filt_150301-150412_30rlks_168alks.unw.geo.vrt"
    vrt_file = glob("{}/filt_*-*_30rlks_168alks.unw.geo.vrt".format(insar_dir))[0]
    lats = ifg_utils.get_geocoded_lats(vrt_file)
    print(lats)
    print("lats : {}".format(lats))
    print("max(lats) : {} : {}".format(max(lats), ifg_utils.convert_number(max(lats))))
    print("min(lats) : {} : {}".format(min(lats), ifg_utils.convert_number(min(lats))))
    print("sorted(lats)[-2] : {} : {}".format(sorted(lats)[-2], ifg_utils.convert_number(sorted(lats)[-2])))
    print("sorted(lats)[1]  {} : {}".format(sorted(lats)[1], ifg_utils.convert_number(sorted(lats)[1])))

    print("sat_direction : {}".format(sat_direction))

    west_lat= "{}_{}".format(ifg_utils.convert_number(sorted(lats)[-2]), ifg_utils.convert_number(min(lats)))

    #IFG_ID_ALOS2_TMPL = "ALOS2-GUNW-{}-{}-{:03d}-scansar-{}_{}-{}-{}-PP-{}-{}"
    # id = IFG_ID_ALOS2_TMPL.format(sat_direction, dt_string, ifg_hash, version.replace('.', '_') )
    # ifg_id = IFG_ID_SP_TMPL.format(sat_direction, "R", track, master_ifg_dt.split('T')[0], slave_ifg_dt.split('T')[0], acq_center_time, west_lat, ifg_hash, version.replace('.', '_'))
    id = IFG_ID_ALOS2_TMPL.format(sat_direction, "R", ref_md['track'], ref_dt, sec_dt, acq_center_time, west_lat, ifg_hash, version.replace('.', '_') ) 
    prod_dir = id
    logger.info("prod_dir : %s" %prod_dir)
    
    ifg_utils.change_dir(wd)
    os.makedirs(prod_dir, 0o755)

    # chdir back up to work directory
    ifg_utils.change_dir(wd)

    #Copy the producta
    for name in glob("{}/*".format(insar_dir)):
        input_path = os.path.join(insar_dir, name)
        if os.path.isfile(input_path):
            logger.info("Copying {} to {}".format(input_path,  prod_dir))
            shutil.copy(input_path,  prod_dir)    
    
    '''
    for name in glob("{}/*".format(insar_dir)):
        logger.info("Copying {} to {}".format(os.path.join(insar_dir, name),  prod_dir))
        shutil.copy(os.path.join(insar_dir, name),  prod_dir)

    
    for name in glob("{}/diff_*".format(insar_dir)):
        logger.info("Copying {} to {}".format(os.path.join(insar_dir, name),  prod_dir))
        shutil.copy(os.path.join(insar_dir, name),  prod_dir)


    for name in glob("{}/*.slc.par.xml".format(insar_dir)):
        logger.info("Copying {} to {}".format(os.path.join(insar_dir, name),  prod_dir))
        shutil.copy(os.path.join(insar_dir, name),  prod_dir)

    for name in glob("{}/*.xml".format(insar_dir)):
        logger.info("Copying {} to {}".format(os.path.join(insar_dir, name),  prod_dir))
        shutil.copy(os.path.join(insar_dir, name),  prod_dir)
    '''

    shutil.copyfile("_context.json", os.path.join(prod_dir,"{}.context.json".format(id)))

    # generate met file
    met_file = os.path.join(prod_dir, "{}.met.json".format(id))
    with open(met_file, 'w') as f: json.dump(ifg_md, f, indent=2)

    # generate dataset JSON
    ds_file = os.path.join(prod_dir, "{}.dataset.json".format(id))
    logger.info("creating dataset file : %s" %ds_file)
    ifg_utils.create_dataset_json(id, version, met_file, ds_file)
 
    #alos2_packagina(id)
    ifg_utils.change_dir(wd)
 
    isce_functions_alos2.create_alos2_md_file("reference", "ref_alos2_md.json")
    isce_functions_alos2.create_alos2_md_file("secondary", "sec_alos2_md.json")



    #ALOS2 metadata.h5 creation
    os.chdir(prod_dir)
    ls_cmd = ["ls", "-l"]
    check_call(ls_cmd, shell=True)

    mgc_cmd = [
        "{}/makeAlos2Geocube.py".format(BASE_PATH),  "-m", "../reference", "-s", "../secondary",  "-o", "metadata.h5"
    ]
    mgc_cmd_line = " ".join(mgc_cmd)
    logger.info("Calling makeAlos2Geocube.py: {}".format(mgc_cmd_line))
    check_call(mgc_cmd_line, shell=True)

    #$BASE_PATH/makeAlos2Geocube.py -m ../reference -s ../secondary -o metadata.h5
    #ALOS2 PACKAGING
    alos2_prod_file = "{}.nc".format(id)
    with open(os.path.join(BASE_PATH, "alos2_groups.json")) as f:
        alos2_cfg = json.load(f)
    alos2_cfg['filename'] = alos2_prod_file
    with open('alos2_groups.json', 'w') as f:
        json.dump(alos2_cfg, f, indent=2, sort_keys=True)

    alos2_cmd = [
        "{}/alos2_packaging.py".format(BASE_PATH)
    ]
    alos2_cmd_line = " ".join(alos2_cmd)
    logger.info("Calling alos2_packaging.py: {}".format(alos2_cmd_line))
    check_call(alos2_cmd_line, shell=True)

    # chdir back up to work directory
    ifg_utils.change_dir(wd)

    #Create Browse Image
    uu = UrlUtils()

    # generate GDAL (ENVI) headers and move to product directory
    raster_prods = (
        'insar/150412-150301_30rlks_168alks.cor',
        'insar/diff_150412-150301_30rlks_168alks.int',
        'insar/ilt_150412-150301_30rlks_168alks.int',
        glob('insar/filt_*-*_*rlks_*alks.unw')[0],
        glob('insar/filt_*-*_*rlks_*alks.unw.conncomp')[0],
        glob('insar/*-*_*rlks_*alks.phsig')[0],
        glob('insar/*-*_*rlks_*alks.los')[0],
        #'insar/los.rdr',
        'insar/crop.dem',
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
    vrt_prod = get_image(glob('insar/filt_*-*_*rlks_*alks.unw.geo.xml')[0])
    vrt_prod_size = get_size(vrt_prod)
    #flat_vrt_prod = get_image("insar/filt_topophase.flat.geo.xml")
    flat_vrt_prod = get_image(glob('insar/filt_*-*_*rlks_*alks.int.geo.xml')[0])
    flat_vrt_prod_size = get_size(flat_vrt_prod)

    print("flat_vrt_prod_size : {}".format(flat_vrt_prod_size))
    print("vrt_prod_size : {}".format(vrt_prod_size))

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

    # read in ionospheric phase
    iono_vrt = glob('insar/*-*_*rlks_*alks.ion.geo.vrt')[0]
    iono = gdal.Open(iono_vrt)
    iono_data = iono.ReadAsArray()
    iono = None

    # read in wrapped interferograma
    if "insar" not in flat_vrt_prod.filename:
        flat_vrt_prod.filename = os.path.join("insar", flat_vrt_prod.filename)
    flat_vrt_prod_shape = (flat_vrt_prod_size['lat']['size'], flat_vrt_prod_size['lon']['size'])
    flat_vrt_prod_im = np.memmap(flat_vrt_prod.filename,
                            dtype=flat_vrt_prod.toNumpyDataType(),
                            mode='c', shape=(flat_vrt_prod_size['lat']['size'], flat_vrt_prod_size['lon']['size']))
    phase = np.angle(flat_vrt_prod_im)

    #remove ionosphere from interfergram phasea
    iono_data[phase==0]=0
    phase = np.angle(np.exp(1j*(phase-iono_data)))
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
    cc_vrt = glob('insar/filt_*2-*_*rlks_*alks.unw.conncomp.geo.vrt')[0]
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
    masked_filt = "filt_topophase.masked.unw.geo"
    #masked_filt =  glob('insar/filt_*-*_*rlks_*alks_msk.unw')[0]
    masked_filt_xml = "filt_topophase.masked.unw.geo.xml"
    #masked_filt_xml = glob('insar/filt_*-*_*rlks_*alks_msk.unw.xml')[0]
    tim = np.memmap(masked_filt, dtype=vrt_prod.toNumpyDataType(), mode='w+', shape=vrt_prod_shape)
    tim[:,:,:] = im1
    im  = Image()
    with open(glob('insar/filt_*-*_*rlks_*alks.unw.geo.xml')[0]) as f:
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
    #vrt_prod_file = glob('insar/filt_*-*_*rlks_*alks_msk.unw.vrt')[0]
    vrt_prod_file = "filt_topophase.masked.unw.geo.vrt"
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
    for i in glob("{}/{}.*.browse*.aux.xml".format(prod_dir, id)): 
        try:
            print("unlinking {}".format(i))
            os.unlink(i)
        except Exception as err:
            print(str)
    

if __name__ == '__main__':
    complete_start_time=datetime.now()
    logger.info("Alos2App End Time : {}".format(complete_start_time))
    cwd = os.getcwd()

    ctx_file = os.path.abspath('_context.json')
    if not os.path.exists(ctx_file):
        raise RuntimeError("Failed to find _context.json.")
    with open(ctx_file) as f:
        ctx = json.load(f)

    main()
