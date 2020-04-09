#!/usr/bin/env python3
"""
Download and/or stitch NED1 or NED13 DEMs, e.g.
ned_dem.py -a download -b 36 42 -123 -120 -n <user> -w <password> -u \
http://grfn-v2-ops-product-bucket.s3-website-us-west-2.amazonaws.com/datasets/dem/ned1/
"""

from builtins import str
from builtins import range
import os, sys, math, json, logging, argparse, zipfile
from subprocess import check_call, CalledProcessError
from itertools import chain
from string import Template
import isce
import isceobj


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('ned_dem')


XML_TMPL = """<component name = "Dem">
    <property name="BYTE_ORDER">
        <value>l</value>
    </property>
    <property name="ACCESS_MODE">
        <value>read</value>
    </property>
    <property name="REFERENCE">
        <value>WGS84</value>
    </property>
    <property name="DATA_TYPE">
        <value>FLOAT</value>
    </property>
    <property name="IMAGE_TYPE">
        <value>dem</value>
    </property>
    <property name="FILE_NAME">
        <value>$dem_file</value>
    </property>
    <property name="DELTA_LONGITUDE">
        <value>$delta_lon</value>
    </property>
 
    <component name="Coordinate1">
        <factorymodule>isceobj.Image</factorymodule>
        <factoryname>createCoordinate</factoryname>
        <doc>First coordinate of a 2D image (width).</doc>
        <property name="startingValue">
            <value>$lon</value>
            <doc>Starting value of the coordinate.</doc>
            <units>degree</units>
        </property>
        <property name="delta">
            <value>$delta_lon</value>
            <doc>Coordinate quantization.</doc>
        </property>
        <property name="size">
            <value>$width</value>
            <doc>Coordinate size.</doc>
        </property>
    </component>
    <property name="NUMBER_BANDS">
        <value>1</value>
    </property>
 
    <component name="Coordinate2">
        <factorymodule>isceobj.Image</factorymodule>
        <factoryname>createCoordinate</factoryname>
        <doc>Second coordinate of a 2D image (length).</doc>
        <property name="startingValue">
            <value>$lat</value>
            <doc>Starting value of the coordinate.</doc>
            <units>degree</units>
        </property>
        <property name="delta">
            <value>$delta_lat</value>
            <doc>Coordinate quantization.</doc>
        </property>
        <property name="size">
            <value>$length</value>
            <doc>Coordinate size.</doc>
        </property>
    </component>
 
    <property name="WIDTH">
        <value>$width</value>
    </property>
    <property name="LENGTH">
        <value>$length</value>
    </property>
    <property name="FIRST_LONGITUDE">
        <value>$lon</value>
    </property>
    <property name="DELTA_LATITUDE">
        <value>$delta_lat</value>
    </property>
    <property name="SCHEME">
        <value>BSQ</value>
    </property>
    <property name="FIRST_LATITUDE">
        <value>$lat</value>
    </property>
</component>
"""


def convert_coord_to_str(lat, lon):
    """Convert coordinate to string for DEM filename."""

    # cribbed from ISCE's DemStitcher class
    if(lon > 180): lon = -(360 - lon)
    if(lon < 0): ew = 'W'
    else: ew = 'E'
    lonAbs = int(math.fabs(lon))
    if(lonAbs >= 100): ew += str(lonAbs)
    elif(lonAbs < 10): ew +=  '00' + str(lonAbs)
    else: ew +=  '0' + str(lonAbs)
    if(int(lat) >= 0): ns = 'N'
    else: ns = 'S'
    latAbs = int(math.fabs(lat))
    if(latAbs >= 10): ns += str(latAbs)
    else: ns += '0' +str(latAbs)
    return ns,ew


def create_file_name(lat, lon):
    """Get DEM filename for a specific lat/lon."""

    # cribbed from ISCE's DemStitcher class
    if lon > 180: lon = -(360 - lon)
    else: lon = lon
    ns, ew = convert_coord_to_str(lat, lon)
    return ns + ew +  '.hgt' +  '.zip'


def get_name_list(lats, lons, url_base):
    """Get url list of DEMs."""

    # cribbed from ISCE's DemStitcher class
    inputFileList = []
    urlFileList = []
    if lons[0] > 180: lons[0] = -(360 - lons[0])
    else: lons[0] = lons[0]
    if lons[1] > 180: lons[1] = -(360 - lons[1])
    else: lons[1] = lons[1]
    lonMin = min(lons[0], lons[1])
    lons[1] = int(math.ceil(max(lons[0], lons[1])))
    lons[0] = int(math.floor(lonMin))
    #sanity check for lat
    latMin = min(lats[0], lats[1])
    lats[1] = int(math.ceil(max(lats[0], lats[1])))
    lats[0] = int(math.floor(latMin))
    # give error if crossing 180 and -180.
    latList = []
    lonList = []
    for i in range(lats[0], lats[1]): # this leave out lats[1], but is ok because the last frame will go up to that point
        latList.append(i)
    #for lat go north to south
    latList.reverse()
    # create the list starting from the min to the max
    if(lons[1] - lons[0] < 180):
        for i in range(lons[0], lons[1]): # this leave out lons[1], but is ok because the last frame will go up to that point
            lonList.append(i)
    else:
        e = "Error. The crossing of E180 and W180 is not handled."
        logger.error(e)
        raise RuntimeError
    latLonList = []
    for lat in latList:
        for lon in lonList:
            name = create_file_name(lat, lon)
            inputFileList.append(name)
            url = os.path.join(url_base, name)
            urlFileList.append(url)
            latLonList.append([lat, lon])
    return urlFileList, len(latList), len(lonList)


def download(url_list, username, password):
    """Download dems."""

    # cribbed from ISCE's DemStitcher class
    if(username is None or password is None):
        if os.path.exists(os.path.join(os.environ['HOME'], '.netrc')):
            command = 'curl -n  -L -c $HOME/.earthdatacookie -b $HOME/.earthdatacookie -k -f -O '
        else:
            logger.error('Please create a .netrc file in your home directory containing ' + \
                         'machine urs.earthdata.nasa.gov\n\tlogin yourusername\n\t' + \
                         'password yourpassword')
            sys.exit(1)
    else:
        command = 'curl -k -f -u ' + username + ':' + password + ' -O '
    dem_files = []
    for url in url_list:
        dem_file = os.path.basename(url)
        if os.path.exists(dem_file):
            dem_files.append(dem_file)
            continue
        try:
            if os.system(command + url): raise Exception
            dem_files.append(dem_file)
        except Exception as e:
            logger.warning('There was a problem in retrieving the file %s: %s' % (url, str(e)))
    return dem_files


def stitch(bbox, dem_files, downsample=None):
    """Stitch NED1/NED13 dems."""

    # unzip dem zip files
    extracted_files = []
    for dem_file in dem_files:
        zip = zipfile.ZipFile(dem_file)
        extracted_files.extend(zip.namelist())
        zip.extractall()
    logger.info("extracted_files: {}".format(extracted_files))

    # merge dems
    check_call("gdalbuildvrt combinedDEM.vrt *.hgt", shell=True)
    if downsample is None: outsize_opt = ""
    else: outsize_opt = "-outsize {} {}".format(downsample, downsample)
    #check_call("gdal_translate -of ENVI {} -projwin {} {} {} {} combinedDEM.vrt stitched.dem".format(outsize_opt, bbox[2], bbox[0], bbox[3], bbox[1]), shell=True)
    check_call("gdalwarp combinedDEM.vrt -te {} {} {} {} -of ENVI {} stitched.dem".format( bbox[2], bbox[0], bbox[3], bbox[1], outsize_opt), shell=True)
    #updte data to fill extream values with default value(-32768). First create a new dem file with the update
    #check_call('gdal_calc.py -A stitched.dem --outfile=stitched_new.dem --calc="-32768*(A<-32768)+A*(A>=-32768)"', shell=True) 
    check_call('gdal_calc.py --format=ENVI -A stitched.dem --outfile=stitchedFix.dem --calc="A*(A>-1000)" --NoDataValue=0', shell=True)
    logger.info("Created stitchedFix.dem with updated value")  
    #check_call('gdal_translate -of vrt stitchedFix.dem stitchedFix.dem.vrt', shell=True)
    #logger.info("Created stitchedFix.dem.vrt with updated value")

    #check_call("gdal2isce_xml.py -i stitchedFix.dem", shell=True) 

    #switch the new with the origional
    rename_file('stitched.dem', 'stitchedFix.dem')
    #rename_file('stitched.dem.vrt', 'stitchedFix.dem.vrt')
    #rename_file('stitched.dem.xml', 'stitchedFix.dem.xml')
    #rename_file('stitched.dem.aux.xml', 'stitchedFix.dem.aux.xml')
    #rename_file('stitched.hdr', 'stitchedFix.hdr')
    logger.info("New Dem file is renamed as original stitched.dem file")
    check_call('gdal_translate -of vrt stitched.dem stitched.dem.vrt', shell=True)


    check_call("gdalinfo -json stitched.dem > stitched.dem.json", shell=True)
    with open('stitched.dem.json') as f:
        info = json.load(f)
    s = Template(XML_TMPL)
    xml = s.substitute(dem_file="stitched.dem", width=info['size'][0], length=info['size'][1],
                       lon=info['geoTransform'][0], delta_lon=info['geoTransform'][1],
                       lat=info['geoTransform'][3], delta_lat=info['geoTransform'][5])
    with open('stitched.dem.xml', 'w') as f:
        f.write(xml)

    # clean out extracted dems
    for i in chain(dem_files, extracted_files): os.unlink(i)

def rename_file(orig_file, new_file):
    bak_dir = os.path.join(os.getcwd(),'bak')
    if not os.path.exists(bak_dir):
        os.makedirs(bak_dir)
    orig_file_path = os.path.join(os.getcwd(), orig_file)
    new_file_path = os.path.join(os.getcwd(), new_file)
    bak_file_path = os.path.join(bak_dir, orig_file+".bak")
    if os.path.isfile(new_file_path):
        if os.path.isfile(orig_file_path):
            os.rename(orig_file_path, bak_file_path)
        os.rename(new_file_path, orig_file_path)
        logger.info("NED-DEM: Renamed %s to %s" %(new_file, orig_file))

def main(url_base, username, password, action, bbox, downsample):
    """Main."""

    logger.info("url_base: {}".format(url_base))
    logger.info("username: {}".format(username))
    logger.info("password: {}".format(password))
    logger.info("action: {}".format(action))
    logger.info("bbox: {}".format(bbox))
    logger.info("downsample: {}".format(downsample))

    # get list of urls
    lats = bbox[0:2]
    lons = bbox[2:4]
    url_list, num_lat, num_lon = get_name_list(lats, lons, url_base)
    logger.info("url_list: {}".format(json.dumps(url_list, indent=2)))
    logger.info("num_lat: {}".format(num_lat))
    logger.info("num_lon: {}".format(num_lon))

    # download list of urls
    dem_files = download(url_list, username, password)
    logger.info("dem_files: {}".format(json.dumps(dem_files), indent=2))

    # stitch
    if action == 'stitch':
        stitched_dem = stitch(bbox, dem_files, downsample)
        logger.info("stitched_dem: {}".format(stitched_dem))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url_base", help="NED DEM url base")
    parser.add_argument("-a", "--action", dest="action", help="action to perform",
                        default="download", choices=["download", "stitch"])
    parser.add_argument("-b", "--bbox", dest="bbox", type=int, required=True,
                        nargs=4, help="Defines the spatial region in the format south " + \
                                      "north west east. The values should be integers " + \
                                      "from (-90,90) for latitudes and (0,360) or " + \
                                      "(-180,180) for longitudes.")
    parser.add_argument("-d", "--downsample", dest="downsample", default=None,
                        help="downsample DEM by a percentage, e.g. 33%")
    parser.add_argument("-u", "--username", dest="username", help="username")
    parser.add_argument("-p", "--password", dest="password", help="password")
    args = parser.parse_args()
    sys.exit(main(args.url_base, args.username, args.password, args.action, 
                  args.bbox, args.downsample))
