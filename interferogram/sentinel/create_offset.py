#!/usr/bin/env python3 
from builtins import str
import os, sys, re, requests, json, shutil, traceback, logging, hashlib, math
from itertools import chain
from zipfile import ZipFile
from subprocess import check_call, CalledProcessError
from glob import glob
from lxml.etree import parse
import numpy as np
from datetime import datetime

from utils.UrlUtils import UrlUtils
from check_interferogram import check_int
from create_input_xml_offset import create_input_xml_offset


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('create_offset')


BASE_PATH = os.path.dirname(__file__)


KILAUEA_DEM_XML = "https://aria-alt-dav.jpl.nasa.gov/repository/products/kilauea/dem_kilauea.dem.xml"
KILAUEA_DEM = "https://aria-alt-dav.jpl.nasa.gov/repository/products/kilauea/dem_kilauea.dem"


MISSION_RE = re.compile(r'^(S1\w)_')
POL_RE = re.compile(r'^S1\w_IW_SLC._1S(\w{2})_')


def get_version():
    """Get dataset version."""

    DS_VERS_CFG = os.path.normpath(
                      os.path.join(
                          os.path.dirname(os.path.abspath(__file__)),
                          '..', '..', 'conf', 'dataset_versions.json'))
    with open(DS_VERS_CFG) as f:
        ds_vers = json.load(f)
    return ds_vers['S1-IFG']


def create_dataset_json(id, version, met_file, ds_file):
    """Write dataset json."""

    # get metadata
    with open(met_file) as f:
        md = json.load(f)

    # build dataset
    ds = {
        'creation_timestamp': "%sZ" % datetime.utcnow().isoformat(),
        'version': version,
        'label': id,
        'location': {
            'type': 'Polygon',
            'coordinates': [
                [   
                    [ md['bbox'][0][1], md['bbox'][0][0] ],
                    [ md['bbox'][1][1], md['bbox'][1][0] ],
                    [ md['bbox'][3][1], md['bbox'][3][0] ],
                    [ md['bbox'][2][1], md['bbox'][2][0] ],
                    [ md['bbox'][0][1], md['bbox'][0][0] ]
                ]
            ]
        }
    }

    # set earliest sensing start to starttime and latest sensing stop to endtime
    if isinstance(md['sensingStart'], str):
        ds['starttime'] = md['sensingStart']
    else:
        md['sensingStart'].sort()
        ds['starttime'] = md['sensingStart'][0]

    if isinstance(md['sensingStop'], str):
        ds['endtime'] = md['sensingStop']
    else:
        md['sensingStop'].sort()
        ds['endtime'] = md['sensingStop'][-1]

    # write out dataset json
    with open(ds_file, 'w') as f:
        json.dump(ds, f, indent=2)


def ifg_exists(es_url, es_index, id):
    """Check interferogram exists in GRQ."""

    total, id = check_int(es_url, es_index, id)
    if total > 0: return True
    return False


def download_file(url, outdir='.', session=None):
    """Download file to specified directory."""
    if session is None: session = requests.session()
    path = os.path.join(outdir, os.path.basename(url))
    logger.info('Downloading URL: {}'.format(url))
    r = session.get(url, stream=True, verify=False)
    try:
        val = r.raise_for_status()
        success = True
    except:
        success = False
    if success:
        with open(path,'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
    return success



def call_noerr(cmd):
    """Run command and warn if exit status is not 0."""

    try: check_call(cmd, shell=True)
    except Exception as e:
        logger.warn("Got exception running {}: {}".format(cmd, str(e)))
        logger.warn("Traceback: {}".format(traceback.format_exc()))


def main():
    """HySDS PGE wrapper for TopsInSAR interferogram generation."""

    # save cwd (working directory)
    cwd = os.getcwd()

    # get context
    ctx_file = os.path.abspath('_context.json')
    if not os.path.exists(ctx_file):
        raise RuntimeError("Failed to find _context.json.")
    with open(ctx_file) as f:
        ctx = json.load(f)
    logger.info("ctx: {}".format(json.dumps(ctx, indent=2)))

    master_safe_dirs = []
    for i in ctx['master_zip_file']:
        master_safe_dirs.append(i.replace(".zip", ".SAFE"))
    slave_safe_dirs = []
    for i in ctx['slave_zip_file']:
        slave_safe_dirs.append(i.replace(".zip", ".SAFE"))

    # unzip SAFE dirs
    master_safe_dirs = []
    for i in ctx['master_zip_file']:
        logger.info("Unzipping {}.".format(i))
        with ZipFile(i, 'r') as zf:
            zf.extractall()
        logger.info("Removing {}.".format(i))
        try: os.unlink(i)
        except: pass
        master_safe_dirs.append(i.replace(".zip", ".SAFE"))
    slave_safe_dirs = []
    for i in ctx['slave_zip_file']:
        logger.info("Unzipping {}.".format(i))
        with ZipFile(i, 'r') as zf:
            zf.extractall()
        logger.info("Removing {}.".format(i))
        try: os.unlink(i)
        except: pass
        slave_safe_dirs.append(i.replace(".zip", ".SAFE"))
    #bbox only needed to create dem and not if dem provided
    if 'dem_urls' not  in ctx:
        # get union bbox
        logger.info("Determining envelope bbox from SLC swaths.")
        bbox_json = "bbox.json"
        bbox_cmd_tmpl = "{}/get_union_bbox.sh -o {} *.SAFE/annotation/s1?-iw{}-slc-{}-*.xml"
        check_call(bbox_cmd_tmpl.format(BASE_PATH, bbox_json, ctx['swathnum'],
                                        "hh"), shell=True)
        with open(bbox_json) as f:
            bbox = json.load(f)['envelope']
        logger.info("bbox: {}".format(bbox))
    # get id base
    id_base = ctx['id']
    logger.info("Product base ID: {}".format(id_base))
    
    # get dataset version and set dataset ID
    version = get_version()
    id = "{}-{}-{}".format(id_base, version, re.sub("[^a-zA-Z0-9_]", "_", ctx.get("context",{})
                                               .get("dataset_tag","standard")))

    # get endpoint configurations
    uu = UrlUtils()
    es_url = uu.rest_url
    es_index = "{}_{}_s1-ifg".format(uu.grq_index_prefix, version)

    # check if interferogram already exists
    logger.info("GRQ url: {}".format(es_url))
    logger.info("GRQ index: {}".format(es_index))
    logger.info("Product ID for version {}: {}".format(version, id))
    #TAGREMOVE
    
    #this part needs to be adapted for offest products, probably just need to add
    #the dataset_tag to _context.json
    if ifg_exists(es_url, es_index, id):
        logger.info("{} interferogram for {}".format(version, id_base) +
                    " was previously generated and exists in GRQ database.")

        # cleanup SAFE dirs
        for i in chain(master_safe_dirs, slave_safe_dirs):
            logger.info("Removing {}.".format(i))
            try: shutil.rmtree(i)
            except: pass
        return 0
    
    # get DEM configuration
    dem_type = ctx.get("context", {}).get("dem_type", "SRTM+v3")
    srtm_dem_url = uu.dem_url
    ned1_dem_url = uu.ned1_dem_url
    ned13_dem_url = uu.ned13_dem_url
    dem_user = uu.dem_u
    dem_pass = uu.dem_p
     
    # download project specific DEM
    if 'dem_urls' in ctx:
        s = requests.session()
        #assume that the first is the data and the second the metadata
        download_file(ctx['dem_urls'][0], session=s)
        download_file(ctx['dem_urls'][1], session=s)
        dem_file = os.path.basename(ctx['dem_urls'][0])
    else:
        # get DEM bbox
        dem_S, dem_N, dem_W, dem_E = bbox
        dem_S = int(math.floor(dem_S))
        dem_N = int(math.ceil(dem_N))
        dem_W = int(math.floor(dem_W))
        dem_E = int(math.ceil(dem_E))
        if dem_type == "SRTM+v3":
            dem_url = srtm_dem_url
            dem_cmd = [
                "{}/applications/dem.py".format(os.environ['ISCE_HOME']), "-a",
                "stitch", "-b", "{} {} {} {}".format(dem_S, dem_N, dem_W, dem_E),
                "-r", "-s", "1", "-f", "-x", "-c", "-n", dem_user, "-w", dem_pass,
                "-u", dem_url
            ]
            dem_cmd_line = " ".join(dem_cmd)
            logger.info("Calling dem.py: {}".format(dem_cmd_line))
            check_call(dem_cmd_line, shell=True)
            dem_file = glob("*.dem.wgs84")[0]
        else:
            if dem_type == "NED1": dem_url = ned1_dem_url
            elif dem_type.startswith("NED13"): dem_url = ned13_dem_url
            else: raise RuntimeError("Unknown dem type %s." % dem_type)
            if dem_type == "NED13-downsampled": downsample_option = "-d 33%"
            else: downsample_option = ""
            dem_cmd = [
                "{}/ned_dem.py".format(BASE_PATH), "-a",
                "stitch", "-b", "{} {} {} {}".format(dem_S, dem_N, dem_W, dem_E),
                downsample_option, "-u", dem_user, "-p", dem_pass, dem_url
            ]
            dem_cmd_line = " ".join(dem_cmd)
            logger.info("Calling ned_dem.py: {}".format(dem_cmd_line))
            check_call(dem_cmd_line, shell=True)
            dem_file = "stitched.dem"
    logger.info("Using DEM file: {}".format(dem_file))

   
    # fix file path in DEM xml
    fix_cmd = [
        "{}/applications/fixImageXml.py".format(os.environ['ISCE_HOME']),
        "-i", dem_file, "--full"
    ]
    fix_cmd_line = " ".join(fix_cmd)
    logger.info("Calling fixImageXml.py: {}".format(fix_cmd_line))
    check_call(fix_cmd_line, shell=True)
        
    # download auciliary calibration files
    aux_cmd = [
        #"{}/fetchCal.py".format(BASE_PATH), "-o", "aux_cal"
        "{}/fetchCalES.py".format(BASE_PATH), "-o", "aux_cal"
    ]
    aux_cmd_line = " ".join(aux_cmd)
    #logger.info("Calling fetchCal.py: {}".format(aux_cmd_line))
    logger.info("Calling fetchCalES.py: {}".format(aux_cmd_line))
    check_call(aux_cmd_line, shell=True)
        
    # create initial input xml
    xml_file = "topsApp.xml"
    create_input_xml_offset(os.path.join(BASE_PATH, 'topsApp_offset.xml.tmpl'), xml_file,
                     str(master_safe_dirs), str(slave_safe_dirs), 
                     ctx['master_orbit_file'], ctx['slave_orbit_file'],dem_file,
                     ctx['swathnum'],
                     ctx['ampcor_skip_width'], ctx['ampcor_skip_height'],
                     ctx['ampcor_src_win_width'], ctx['ampcor_src_win_height'],
                     ctx['ampcor_src_width'], ctx['ampcor_src_height'])

    # run topsApp for offset
    topsapp_cmd = [
        "topsApp.py", "--steps"
    ]
    topsapp_cmd_line = " ".join(topsapp_cmd)
    logger.info("Calling topsApp.py  for offest: {}".format(topsapp_cmd_line))
    check_call(topsapp_cmd_line, shell=True)
    
    # create product directory
    prod_dir = id
    os.makedirs(prod_dir, 0o755)

    # create merged directory in product
    prod_merged_dir = os.path.join(prod_dir, 'merged')
    os.makedirs(prod_merged_dir, 0o755)

    # generate GDAL (ENVI) headers and move to product directory
    raster_prods = (
        'merged/dense_offsets.bil',
        'merged/dense_offsets_snr.bil',
        'merged/filt_dense_offsets.bil',
        'merged/los.rdr'
    )
    for i in raster_prods:
        # radar-coded products
        call_noerr("isce2gis.py envi -i {}".format(i))
        #call_noerr("gdal_translate {} {}.tif".format(i, i))
        gdal_xml = "{}.xml".format(i)
        gdal_hdr = "{}.hdr".format(i)
        #gdal_tif = "{}.tif".format(i)
        gdal_vrt = "{}.vrt".format(i)
        if os.path.exists(i): shutil.move(i, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(i))
        if os.path.exists(gdal_xml): shutil.move(gdal_xml, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_xml))
        if os.path.exists(gdal_hdr): shutil.move(gdal_hdr, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_hdr))
        #if os.path.exists(gdal_tif): shutil.move(gdal_tif, prod_merged_dir)
        #else: logger.warn("{} wasn't generated.".format(gdal_tif))
        if os.path.exists(gdal_vrt): shutil.move(gdal_vrt, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_vrt))

        # geo-coded products
        j = "{}.geo".format(i)
        if not os.path.exists(j): continue
        call_noerr("isce2gis.py envi -i {}".format(j))
        #call_noerr("gdal_translate {} {}.tif".format(j, j))
        gdal_xml = "{}.xml".format(j)
        gdal_hdr = "{}.hdr".format(j)
        #gdal_tif = "{}.tif".format(j)
        gdal_vrt = "{}.vrt".format(j)
        if os.path.exists(j): shutil.move(j, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(j))
        if os.path.exists(gdal_xml): shutil.move(gdal_xml, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_xml))
        if os.path.exists(gdal_hdr): shutil.move(gdal_hdr, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_hdr))
        #if os.path.exists(gdal_tif): shutil.move(gdal_tif, prod_merged_dir)
        #else: logger.warn("{} wasn't generated.".format(gdal_tif))
        if os.path.exists(gdal_vrt): shutil.move(gdal_vrt, prod_merged_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_vrt))

    # save other files to product directory
    shutil.copyfile("_context.json", os.path.join(prod_dir,"{}.context.json".format(id)))
    shutil.copyfile("topsApp.xml", os.path.join(prod_dir, "topsApp.xml"))
    shutil.copyfile("fine_interferogram/IW{}.xml".format(ctx['swathnum']),
                    os.path.join(prod_dir, "fine_interferogram.xml"))
    shutil.copyfile("master/IW{}.xml".format(ctx['swathnum']),
                    os.path.join(prod_dir, "master.xml"))
    shutil.copyfile("slave/IW{}.xml".format(ctx['swathnum']),
                    os.path.join(prod_dir, "slave.xml"))
    if os.path.exists('topsProc.xml'):
        shutil.copyfile("topsProc.xml", os.path.join(prod_dir, "topsProc.xml"))
    if os.path.exists('isce.log'):
        shutil.copyfile("isce.log", os.path.join(prod_dir, "isce.log"))

    # move PICKLE to product directory
    shutil.move('PICKLE', prod_dir)
    
    # create browse images
    os.chdir(prod_merged_dir)
    mdx_path = "{}/bin/mdx".format(os.environ['ISCE_HOME'])
    from utils.createImage import createImage
    from isceobj.Image.Image import Image
    offset_file = "dense_offsets.bil.geo"
    snr_file = "dense_offsets_snr.bil.geo"

    im = Image()
    im.load(offset_file + '.xml')
    mdx_args = '-s %d -r4 -rtlr %d -cmap cmy -wrap 10'%(im.width, 4*im.width)
    
    #unwrapped image at different rates
    createImage("{} -P {} {}".format(mdx_path, offset_file,mdx_args),'dense_offsets_az.bil.geo')
    
    mdx_args = '-s %d -r4 -rhdr %d -cmap cmy -wrap 10'%(im.width, 4*im.width)
    #unwrapped image at different rates
    createImage("{} -P {} {}".format(mdx_path, offset_file,mdx_args),'dense_offsets_rn.bil.geo')
    
    mdx_args = '-s %d -r4 -clpmin 0 -clpmax 10 -cmap cmy -wrap 12'%(im.width,)
    #unwrapped image at different rates
    createImage("{} -P {} {}".format(mdx_path, snr_file,mdx_args),snr_file)
   
    # move all browse images to root of product directory
    call_noerr("mv -f *.png ..")

   

    # chdir back up to work directory
    os.chdir(cwd)

    # extract metadata from master
    met_file = os.path.join(prod_dir, "{}.met.json".format(id))
    extract_cmd_path = os.path.abspath(os.path.join(BASE_PATH, '..', 
                                                    '..', 'frameMetadata',
                                                    'sentinel'))
    #TAGREMOVE
    #return
    extract_cmd_tmpl = "{}/extractMetadata_s1.sh -i {}/annotation/s1?-iw{}-slc-{}-*.xml -o {}"
    check_call(extract_cmd_tmpl.format(extract_cmd_path, master_safe_dirs[0],
                                       ctx['swathnum'], "hh", met_file),shell=True)
    
   
    # add master/slave ids and orbits to met JSON (per ASF request)
    master_ids = [i.replace(".zip", "") for i in ctx['master_zip_file']]
    slave_ids = [i.replace(".zip", "") for i in ctx['slave_zip_file']]
    master_rt = parse(os.path.join(prod_dir, "master.xml"))
    master_orbit_number = eval(master_rt.xpath('.//property[@name="orbitnumber"]/value/text()')[0])
    slave_rt = parse(os.path.join(prod_dir, "slave.xml"))
    slave_orbit_number = eval(slave_rt.xpath('.//property[@name="orbitnumber"]/value/text()')[0])
    with open(met_file) as f: md = json.load(f)
    md['master_scenes'] = master_ids
    md['slave_scenes'] = slave_ids
    md['orbitNumber'] = [master_orbit_number, slave_orbit_number]
    md['dataset_type'] = 'offset'
    #fix to make platform metadata consistent
    if 'platform' in md:
        if md['platform'] == 'S1A':
            md['platform'] = 'Sentinel-1A'
        if md['platform'] == 'S1B':
            md['platform'] = 'Sentinel-1B'
    if 'orbit' in md: del md['orbit'] #FIX FOR INVALID ORBIT METADATA
    if ctx.get('stitch_subswaths_xt', False): md['swath'] = [1, 2, 3]
    #md['esd_threshold'] = esd_coh_th if do_esd else -1.  # add ESD coherence threshold

    # add range_looks and azimuth_looks to metadata for stitching purposes
    md['azimuth_looks'] = int(ctx['azimuth_looks'])
    md['range_looks'] = int(ctx['range_looks'])

    # add dem_type
    md['dem_type'] = dem_type

    # write met json
    with open(met_file, 'w') as f: json.dump(md, f, indent=2)
    
    # generate dataset JSON
    ds_file = os.path.join(prod_dir, "{}.dataset.json".format(id))
    create_dataset_json(id, version, met_file, ds_file)
    
    # move merged products to root of product directory
    #call_noerr("mv -f {}/* {}".format(prod_merged_dir, prod_dir))
    #shutil.rmtree(prod_merged_dir)

    # write PROV-ES JSON
    #${BASE_PATH}/create_prov_es-create_interferogram.sh $id $project $master_orbit_file $slave_orbit_file \
    #                                                        ${dem_file}.xml $dem_file $WORK_DIR \
    #                                                        ${id}/${id}.prov_es.json > create_prov_es.log 2>&1
    
    # clean out SAFE directories and DEM files
    for i in chain(master_safe_dirs, slave_safe_dirs): shutil.rmtree(i)
    for i in glob("dem*"): os.unlink(i)


if __name__ == '__main__':
    try: status = main()
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)
