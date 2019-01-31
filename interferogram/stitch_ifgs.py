#!/usr/bin/env python3 
import os, sys, re, requests, json, shutil, traceback, logging, hashlib, math
from itertools import chain
from subprocess import check_call, CalledProcessError
from glob import glob
from lxml.etree import parse
import numpy as np
from datetime import datetime
from osgeo import ogr, osr

from utils.UrlUtils import UrlUtils
from utils.createImage import createImage
from sentinel.check_interferogram import check_int


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('stitch_ifgs')


BASE_PATH = os.path.dirname(__file__)


def get_version():
    """Get dataset version."""

    DS_VERS_CFG = os.path.normpath(
                      os.path.join(
                          os.path.dirname(os.path.abspath(__file__)),
                          '..', 'conf', 'dataset_versions.json'))
    with open(DS_VERS_CFG) as f:
        ds_vers = json.load(f)
    return ds_vers['S1-IFG-STITCHED']


def get_union_polygon(ds_files):
    """Get GeoJSON polygon of union of IFGs."""

    geom_union = None
    for ds_file in ds_files:
         with open(ds_file) as f:
             ds = json.load(f)
         geom = ogr.CreateGeometryFromJson(json.dumps(ds['location'], indent=2, sort_keys=True))
         if geom_union is None: geom_union = geom
         else: geom_union = geom_union.Union(geom)
    return json.loads(geom_union.ExportToJson()), geom_union.GetEnvelope()


def get_times(ds_files):
    """Get starttimes and endtimes."""

    starttimes = []
    endtimes = []
    for ds_file in ds_files:
         with open(ds_file) as f:
             ds = json.load(f)
         starttimes.append(ds['starttime'])
         endtimes.append(ds['endtime'])
    return starttimes, endtimes


def create_dataset_json(id, version, ds_files, ds_json_file):
    """Create HySDS dataset json file."""

    # get union polygon
    location, env = get_union_polygon(ds_files)
    logger.info("union polygon: {}.".format(json.dumps(location, indent=2, sort_keys=True)))

    # get starttime and endtimes
    starttimes, endtimes = get_times(ds_files)
    starttimes.sort()
    endtimes.sort()
    starttime = starttimes[0]
    endtime = endtimes[-1]

    # build dataset
    ds = {
        'creation_timestamp': "%sZ" % datetime.utcnow().isoformat(),
        'version': version,
        'label': id,
        'location': location,
        'starttime': starttime,
        'endtime': endtime,
    }

    # write out dataset json
    with open(ds_json_file, 'w') as f:
        json.dump(ds, f, indent=2)

    # return envelope and times
    return env, starttime, endtime


def create_met_json(id, version, env, starttime, endtime, met_files, met_json_file, direction):
    """Create HySDS met json file."""

    # build met
    bbox = [
        [ env[3], env[0] ],
        [ env[3], env[1] ],
        [ env[2], env[1] ],
        [ env[2], env[0] ],
    ]
    met = {
        'stitch_direction': direction,
        'product_type': 'interferogram',
        'master_scenes': [],
        'refbbox': [],
        'esd_threshold': [],
        'frameID': [],
        'temporal_span': None,
        'swath': [],
        'trackNumber': None,
        'archive_filename': id,
        'dataset_type': 'slc',
        'tile_layers': [ 'amplitude', 'displacement' ],
        'latitudeIndexMin': int(math.floor(env[2] * 10)),
        'latitudeIndexMax': int(math.ceil(env[3] * 10)),
        'parallelBaseline': [],
        'url': [],
        'doppler': [],
        'version': [],
        'slave_scenes': [],
        'orbit_type': [],
        'spacecraftName': [],
        'frameNumber': None,
        'reference': None,
        'bbox': bbox,
        'ogr_bbox': [[x, y] for y, x in bbox],
        'orbitNumber': [],
        'inputFile': 'ifg_stitch.json',
        'perpendicularBaseline': [],
        'orbitRepeat': [],
        'sensingStop': endtime,
        'polarization': [],
        'scene_count': 0,
        'beamID': None,
        'sensor': [],
        'lookDirection': [],
        'platform': [],
        'startingRange': [],
        'frameName': [],
        'tiles': True,
        'sensingStart': starttime,
        'beamMode': [],
        'imageCorners': [],
        'direction': [],
        'prf': [],
        "sha224sum": hashlib.sha224(str.encode(os.path.basename(met_json_file))).hexdigest(),
    }

    # collect values
    set_params = ('master_scenes', 'esd_threshold', 'frameID', 'swath', 'parallelBaseline',
                  'doppler', 'version', 'slave_scenes', 'orbit_type', 'spacecraftName',
                  'orbitNumber', 'perpendicularBaseline', 'orbitRepeat', 'polarization', 
                  'sensor', 'lookDirection', 'platform', 'startingRange',
                  'beamMode', 'direction', 'prf' )
    single_params = ('temporal_span', 'trackNumber')
    list_params = ('master_scenes', 'slave_scenes', 'platform', 'swath', 'perpendicularBaseline', 'parallelBaseline')
    mean_params = ('perpendicularBaseline', 'parallelBaseline')
    for i, met_file in enumerate(met_files):
        with open(met_file) as f:
            md = json.load(f)
        for param in set_params:
            #logger.info("param: {}".format(param))
            if isinstance(md[param], list):
                met[param].extend(md[param])
            else:
                met[param].append(md[param])
        if i == 0:
            for param in single_params:
                met[param] = md[param]
        met['scene_count'] += 1
    for param in set_params:
        tmp_met = list(set(met[param]))
        if param in list_params:
            met[param] = tmp_met
        else:
            met[param] = tmp_met[0] if len(tmp_met) == 1 else tmp_met
    for param in mean_params:
        met[param] = np.mean(met[param])

    # write out dataset json
    with open(met_json_file, 'w') as f:
        json.dump(met, f, indent=2)


def ifg_exists(es_url, es_index, id):
    """Check interferogram exists in GRQ."""

    total, id = check_int(es_url, es_index, id)
    if total > 0: return True
    return False


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
    logger.info("ctx: {}".format(json.dumps(ctx, indent=2, sort_keys=True)))

    # get args
    project = ctx['project']
    direction = ctx.get('direction', 'along')
    extra_products = ctx.get('extra_products', [])
    filenames = ctx['filenames']
    outname = 'filt_topophase.unw.geo'

    # get id base
    id_base = ctx['id']
    logger.info("Product base ID: {}".format(id_base))
    
    # get dataset version and set dataset ID
    version = get_version()
    id = "{}-{}-{}".format(id_base, version, re.sub("[^a-zA-Z0-9_]", "_", ctx.get("context", {})
                                               .get("dataset_tag", "standard")))

    # get endpoint configurations
    uu = UrlUtils()
    es_url = uu.rest_url
    es_index = "{}_{}_s1-ifg-stitched".format(uu.grq_index_prefix, version)

    # check if interferogram already exists
    logger.info("GRQ url: {}".format(es_url))
    logger.info("GRQ index: {}".format(es_index))
    logger.info("Product ID for version {}: {}".format(version, id))
    if ifg_exists(es_url, es_index, id):
        logger.info("{} interferogram for {}".format(version, id_base) +
                    " was previously generated and exists in GRQ database.")

        # cleanup IFG dirs
        for i in [os.path.split(fname)[0] for swath_list in filenames for fname in swath_list]:
            logger.info("Removing {}.".format(i))
            try: shutil.rmtree(i)
            except: pass
        return 0

    # create product directory
    dataset_dir = os.path.abspath(id)
    os.makedirs(dataset_dir, 0o755)

    # dump input file
    inp = {
        'direction': direction,
        'extra_products': extra_products,
        'filenames': filenames,
        'outname': outname,
    }
    ifg_stitch_file = os.path.join(dataset_dir, "ifg_stitch.json")
    with open(ifg_stitch_file, 'w') as f:
        json.dump(inp, f, indent=2)

    # run stitccher
    stc_cmd = [
        "python3", os.path.join(BASE_PATH, "ifg_stitcher.py"), ifg_stitch_file
    ]
    stc_cmd_line = " ".join(stc_cmd)
    logger.info("Calling ifg_stitcher.py: {}".format(stc_cmd_line))
    check_call(stc_cmd_line, shell=True)
        
    # generate GDAL (ENVI) headers and move to product directory
    raster_prods = [
        'filt_topophase.unw.geo',
        'filt_topophase.unw.conncomp.geo',
        'phsig.cor.geo',
    ]
    raster_prods.extend(extra_products)
    for j in raster_prods:
        if not os.path.exists(j): continue
        gdal_xml = "{}.xml".format(j)
        gdal_hdr = "{}.hdr".format(j)
        #gdal_tif = "{}.tif".format(j)
        gdal_vrt = "{}.vrt".format(j)
        if os.path.exists(j): shutil.move(j, dataset_dir)
        else: logger.warn("{} wasn't generated.".format(j))
        if os.path.exists(gdal_xml): shutil.move(gdal_xml, dataset_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_xml))
        if os.path.exists(gdal_hdr): shutil.move(gdal_hdr, dataset_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_hdr))
        if os.path.exists(gdal_vrt): shutil.move(gdal_vrt, dataset_dir)
        else: logger.warn("{} wasn't generated.".format(gdal_vrt))

    # save other files to product directory
    shutil.copyfile("_context.json", os.path.join(dataset_dir,"{}.context.json".format(id)))
    if os.path.exists('isce.log'):
        shutil.copyfile("isce.log", os.path.join(dataset_dir, "isce.log"))
    if os.path.exists('stitch_ifgs.log'):
        shutil.copyfile("stitch_ifgs.log", os.path.join(dataset_dir, "stitch_ifgs.log"))

    # create browse images
    os.chdir(dataset_dir)
    mdx_app_path = "{}/applications/mdx.py".format(os.environ['ISCE_HOME'])
    mdx_path = "{}/bin/mdx".format(os.environ['ISCE_HOME'])
    unw_file = "filt_topophase.unw.geo"

    #unwrapped image at different rates
    createImage("{} -P {}".format(mdx_app_path, unw_file),unw_file)
    #createImage("{} -P {} -wrap {}".format(mdx_app_path, unw_file, rad),unw_file + "_5cm")
    createImage("{} -P {} -wrap 20".format(mdx_app_path, unw_file),unw_file + "_20rad")

    #amplitude image
    unw_xml = "filt_topophase.unw.geo.xml"
    rt = parse(unw_xml)
    size = eval(rt.xpath('.//component[@name="coordinate1"]/property[@name="size"]/value/text()')[0])
    rtlr = size * 4
    logger.info("rtlr value for amplitude browse is: {}".format(rtlr))
    createImage("{} -P {} -s {} -amp -r4 -rtlr {} -CW".format(mdx_path, unw_file, size, rtlr), 'amplitude.geo')

    #coherence image
    #top_file = "topophase.cor.geo"
    #createImage("{} -P {}".format(mdx_app_path, top_file),top_file)

    #should be the same size as unw but just in case
    #top_xml = "topophase.cor.geo.xml"
    #rt = parse(top_xml)
    #size = eval(rt.xpath('.//component[@name="coordinate1"]/property[@name="size"]/value/text()')[0])
    #rhdr = size * 4
    #createImage("{} -P {} -s {} -r4 -rhdr {} -cmap cmy -wrap 1.2".format(mdx_path, top_file,size,rhdr),"topophase_ph_only.cor.geo")

    # create unw KMZ
    unw_kml = "unw.geo.kml"
    unw_kmz = "{}.kmz".format(id)
    call_noerr("{} {} -kml {}".format(mdx_app_path, unw_file, unw_kml))
    call_noerr("{}/sentinel/create_kmz.py {} {}.png {}".format(BASE_PATH, unw_kml, unw_file, unw_kmz))

    # remove kml
    call_noerr("rm -f *.kml")

    # chdir back up to work directory
    os.chdir(cwd)

    # create displacement tile layer
    tiles_dir = "{}/tiles".format(dataset_dir)
    vrt_prod_file = "{}/filt_topophase.unw.geo.vrt".format(dataset_dir)
    tiler_cmd_path = os.path.abspath(os.path.join(BASE_PATH, '..', 'map_tiler'))
    dis_layer = "displacement"
    tiler_cmd_tmpl = "{}/create_tiles.py {} {}/{} -b 2 -m prism --nodata 0"
    call_noerr(tiler_cmd_tmpl.format(tiler_cmd_path, vrt_prod_file, tiles_dir, dis_layer))

    # create amplitude tile layer
    amp_layer = "amplitude"
    tiler_cmd_tmpl = "{}/create_tiles.py {} {}/{} -b 1 -m gray --clim_min 10 --clim_max_pct 80 --nodata 0"
    call_noerr(tiler_cmd_tmpl.format(tiler_cmd_path, vrt_prod_file, tiles_dir, amp_layer))

    # create COG (cloud optimized geotiff) with no_data set
    cog_prod_file = "{}/filt_topophase.unw.geo.tif".format(dataset_dir)
    cog_cmd_tmpl = "gdal_translate {} tmp.tif -co TILED=YES -co COMPRESS=DEFLATE -a_nodata 0"
    check_call(cog_cmd_tmpl.format(vrt_prod_file), shell=True)
    check_call("gdaladdo -r average tmp.tif 2 4 8 16 32", shell=True)
    cog_cmd_tmpl = "gdal_translate tmp.tif {} -co TILED=YES -co COPY_SRC_OVERVIEWS=YES -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 --config GDAL_TIFF_OVR_BLOCKSIZE 512"
    check_call(cog_cmd_tmpl.format(cog_prod_file), shell=True)
    os.unlink("tmp.tif")

    # get list of dataset and met files
    dsets = []
    mets = []
    for i in [os.path.dirname(os.path.dirname(fname)) for swath_list in filenames for fname in swath_list]:
        dsets.append(os.path.join(i, "_{}.dataset.json".format(i)))
        mets.append(os.path.join(i, "_{}.met.json".format(i)))
    logger.info("Datasets: {}.".format(dsets))
    logger.info("Mets: {}.".format(mets))

    # create dataset json
    ds_json_file = os.path.join(dataset_dir, "{}.dataset.json".format(id))
    envelope, starttime, endtime = create_dataset_json(id, version, dsets, ds_json_file)

    # create met json
    met_json_file = os.path.join(dataset_dir, "{}.met.json".format(id))
    create_met_json(id, version, envelope, starttime, endtime, mets, met_json_file, direction)

    # cleanup IFG dirs
    for i in [os.path.split(fname)[0] for swath_list in filenames for fname in swath_list]:
        logger.info("Removing {}.".format(i))
        try: shutil.rmtree(i)
        except: pass


if __name__ == '__main__':
    try: status = main()
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)
