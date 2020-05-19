#!/usr/bin/env python3
# David Bekaert - Jet Propulsion Laboratory
# set of functions that are leveraged in the packaging of the ARIA standard product 

from __future__ import division
from builtins import str
from builtins import range
from past.utils import old_div
import glob
import os
import xml_json_converter

def loadProduct(xmlname):
    '''
    Load the product using Product Manager.
    '''
    # from Cunren's code on extracting track data from alos2App
    import isce, isceobj
    from iscesys.Component.ProductManager import ProductManager as PM

    print("loadProduct(xmlname) : {}".format(xmlname))
    pm = PM()
    pm.configure()
    obj = pm.loadProduct(xmlname)
    return obj


def loadTrack(date):
    '''
    date: YYMMDD
    '''
    # from Cunren's code on extracting track data from alos2App
    track = loadProduct('{}.track.xml'.format(date))
    track.frames = []
    frameParameterFiles = sorted(glob.glob(os.path.join('f*_*', '{}.frame.xml'.format(date))))
    for x in frameParameterFiles:
        track.frames.append(loadProduct(x))
    return track

def getTrackFrameData(track):
    '''
    get frame information 
    '''
    import datetime

    frameData = {}

    numberOfFrames = len(track.frames)
    numberOfSwaths = len(track.frames[0].swaths)

    rangePixelSizeList = []
    sensingStartList = []
    sensingEndList = []
    startingRangeList = []
    endingRangeList = []
    azimuthLineIntervalList =[]
    azimuthPixelSizeList = []
    swaths = []

    for i in range(numberOfFrames):
        for j in range(numberOfSwaths):
            swath = track.frames[i].swaths[j]
            swaths.append(swath)
            rangePixelSizeList.append(swath.rangePixelSize)
            azimuthLineIntervalList.append(swath.azimuthLineInterval)
            azimuthPixelSizeList.append(swath.azimuthPixelSize)
            sensingStartList.append(swath.sensingStart)
            sensingEndList.append(swath.sensingStart + datetime.timedelta(seconds=(swath.numberOfLines-1) * swath.azimuthLineInterval))
            startingRangeList.append(swath.startingRange)
            endingRangeList.append(swath.startingRange + (swath.numberOfSamples - 1) * swath.rangePixelSize)
    azimuthTimeMin = min(sensingStartList)
    azimuthTimeMax = max(sensingEndList)
    azimuthTimeMid = azimuthTimeMin+datetime.timedelta(seconds=(azimuthTimeMax-azimuthTimeMin).total_seconds()/2.0)
    rangeMin = min(startingRangeList)
    rangeMax = max(endingRangeList)
    rangeMid = (rangeMin + rangeMax) / 2.0

    bbox = [rangeMin, rangeMax, azimuthTimeMin, azimuthTimeMax]
    pointingDirection = {'right': -1, 'left': 1}
    
     #####################################
    # in image coordinate
    #         1      2
    #         --------
    #         |      |
    #         |      |
    #         |      |
    #         --------
    #         3      4
    # in geography coorindate
    #        1       2
    #         --------
    #         \       \
    #          \       \
    #           \       \
    #            --------
    #            3       4
    #####################################
    # in image coordinate

    # corner 1
    llh1 = track.orbit.rdr2geo(azimuthTimeMin, rangeMin, height=0, side=pointingDirection[track.pointingDirection])
    # corner 2
    llh2 = track.orbit.rdr2geo(azimuthTimeMin, rangeMax, height=0, side=pointingDirection[track.pointingDirection])
    # corner 3
    llh3 = track.orbit.rdr2geo(azimuthTimeMax, rangeMin, height=0, side=pointingDirection[track.pointingDirection])
    # corner 4
    llh4 = track.orbit.rdr2geo(azimuthTimeMax, rangeMax, height=0, side=pointingDirection[track.pointingDirection])

    # re-sort in geography coordinate
    if track.passDirection.lower() == 'descending':
        if track.pointingDirection.lower() == 'right':
            footprint = [llh2, llh1, llh4, llh3]
        else:
            footprint = [llh1, llh2, llh3, llh4]
    else:
        if track.pointingDirection.lower() == 'right':
            footprint = [llh4, llh3, llh2, llh1]
        else:
            footprint = [llh3, llh4, llh1, llh2]

    
    frameData['numberOfFrames'] = numberOfFrames
    frameData['numberOfSwaths'] = numberOfSwaths 
    frameData['rangePixelSizeList'] = rangePixelSizeList
    frameData['sensingStartList'] = sensingStartList
    frameData['sensingEndList'] = sensingEndList
    frameData['startingRangeList'] = startingRangeList
    frameData['endingRangeList'] = endingRangeList
    frameData['azimuthLineIntervalList'] = azimuthLineIntervalList
    frameData['azimuthPixelSizeList'] = azimuthPixelSizeList
    frameData['bbox'] = bbox
    frameData['footprint'] = footprint
    frameData['swaths'] = swaths
    frameData['rangeMin'] = rangeMin
    frameData['rangeMax'] = rangeMax
    frameData['rangeMid'] = rangeMid

    return frameData


def get_alos2_obj(dir_name):
    import os
    import glob
    import re
    from subprocess import check_call, check_output

    track = None
    img_file = sorted(glob.glob(os.path.join(dir_name, 'IMG*')))

    if len(img_file) > 0:
        match = re.search('IMG-[A-Z]{2}-(ALOS2)(.{05})(.{04})-(\d{6})-.{4}.*',img_file[0])
        if match:
            date = match.group(4)
            create_alos2app_xml(dir_name)
            check_output("alos2App.py --steps --end=preprocess", shell=True)
            track = loadTrack(date)
            track.spacecraftName = match.group(1)
            track.orbitNumber = match.group(2)
            track.frameNumber = match.group(3)

    return track

def get_alos2_bbox(args):
    import json

    ref_json_file = args[0]
    with open (ref_json_file, 'r') as f:
        data = json.load(f)

    return data['bbox']

    
def get_alos2_bbox_from_footprint(footprint):
    bbox = []
    for i in range(len(footprint)):
        bbox.append([footprint[i][0], footprint[i][1]])
    return bbox

def create_alos2_md_json(dirname):
    from scipy.constants import c

    track = get_alos2_obj(dirname)
    frameData = getTrackFrameData(track)
    bbox = frameData['footprint']
    
    md = {}
    md['geometry'] = {
        "coordinates":[[
        bbox[0][1:None:-1], # NorthWest Corner
        bbox[1][1:None:-1], # NorthEast Corner
        bbox[3][1:None:-1], # SouthWest Corner
        bbox[2][1:None:-1], # SouthEast Corner
        bbox[0][1:None:-1],
        ]],
        "type":"Polygon"
    }
    md['sensing_start'] = "{}".format(min(frameData['sensingStartList']).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    md['sensing_stop'] = "{}".format(max(frameData['sensingEndList']).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    md['absolute_orbit'] = track.orbitNumber
    md['frame'] = track.frameNumber
    md['flight_direction'] = 'asc' if 'asc' in track.catalog['passdirection'] else 'dsc'
    md['satellite_name'] = track.spacecraftName
    md['source'] = "isce_preprocessing"
    md['bbox'] = get_alos2_bbox_from_footprint(bbox)
    md['pointing_direction'] = track.catalog['pointingdirection']
    md['radar_wave_length'] = track.catalog['radarwavelength']
    md['starting_range'] = min(frameData['startingRangeList'])
    md['azimuth_pixel_size'] = max(frameData['azimuthPixelSizeList'])
    md['azimuth_line_interval'] = max(frameData['azimuthLineIntervalList'])
    md['frequency'] = old_div(c, track.catalog['radarwavelength'])
    md['orbit_type'] = get_orbit_type(track.orbit.getOrbitQuality())
    md['orbit_source'] = track.orbit.getOrbitSource()
    md['nearRange'] = frameData['rangeMin']
    md['farRange'] = frameData['rangeMax']
    md['rangePixelSize'] = track.catalog['rangepixelsize']	

    return md

def get_orbit_type(orbit_quality):
    if 'precision' in orbit_quality:
        return "POEORB"
    return "RESORB"

def create_alos2_md_file(dirname, filename):
    import json
    md = create_alos2_md_json(dirname)
    #print(md)
    with open(filename, "w") as f:
        json.dump(md, f, indent=2)
        f.close()


def get_alos2_metadata_variable(args):
    '''
        return the value of the requested variable
    '''

    data = None
    masterdir = args[0]
    variable = args[1]

    print("\n\nget_alos2_metadata_variable(args) : {}".format(args))

    alos2_metadata = get_alos2_metadata_reference_json(args[0]) #create_alos2_md_json(masterdir) # get_alos2_metadata(masterdir)
    if variable in alos2_metadata:
        data = alos2_metadata[variable]

    return data

def get_alos2_metadata_reference_json(ref_json_file):
    import json

    data = {}
    with open (ref_json_file, 'r') as f:
        data = json.load(f)
    return data

def get_alos2_metadata(masterdir):
    import pdb
    from scipy.constants import c

    # get a list of avialble xml files for IW*.xml
    IWs = get_alos2_subswath_xml(masterdir)

    # append all swaths togheter
    frames=[]
    for IW  in IWs:
        obj = read_isce_product(IW)
        frames.append(obj)
   
    output={}
    dt = min(frame.sensingStart for frame in frames)
    output['sensingStart'] =  dt.isoformat('T') + 'Z'
    dt = max(frame.sensingStop for frame in frames)
    output['sensingStop'] = dt.isoformat('T') + 'Z'
    output['farRange'] = max(frame.farRange for frame in frames)
    output['startingRange'] = min(frame.startingRange for frame in frames)
    output['spacecraftName'] = obj.spacecraftName 
    burst = obj.bursts[0]   
    output['rangePixelSize'] = burst.rangePixelSize
    output['azimuthTimeInterval'] = burst.azimuthTimeInterval
    output['wavelength'] = burst.radarWavelength
    output['frequency']  = old_div(c,output['wavelength'])
    if "POEORB" in obj.orbit.getOrbitSource():
        output['orbittype'] = "precise"
    elif "RESORB" in obj.orbit.getOrbitSource():
        output['orbittype'] = "restituted"
    else:
        output['orbittype'] = ""
    #output['bbox'] = get_bbox(masterdir)
    # geo transform grt for x y 
    # bandwith changes per swath - placeholder c*b/2 or delete
    # alos2 xml file
    # refer to safe files frame doppler centroid burst[middle].doppler
    # extract from alos2app.xml 

    return output

def data_loading(filename,out_data_type=None,data_band=None):
    """
        GDAL READER of the data
        filename: the gdal readable file that needs to be loaded
        out_data_type: the datatype of the output data, default is original
        out_data_res: the resolution of the output data, default is original
        data_band: the band that needs to be loaded, default is all
    """

    import gdal
    import numpy
    import os 

    # converting to the absolute path
    filename = os.path.abspath(filename)
    if not os.path.isfile(filename):
        print(filename + " does not exist")
        out_data = None
        return out_data

    # open the GDAL file and get typical data information
    try:
        data =  gdal.Open(filename, gdal.GA_ReadOnly)
    except:
        print(filename + " is not a gdal supported file")
        out_data = None
        return out_data

    # loading the requested band or by default all
    if data_band is not None:
        raster = data.GetRasterBand(data_band)
        out_data = raster.ReadAsArray()

    # getting the gdal transform and projection
    geoTrans = str(data.GetGeoTransform())
    projectionRef = str(data.GetProjection())

    # getting the no-data value
    try:
        NoData = data.GetNoDataValue()
        print(NoData)
    except:
        NoData = None

    # change the dataype if provided
    if out_data_type is not None:
        # changing the format if needed
        out_data = out_data.astype(dtype=out_data_type)

    return out_data, geoTrans,projectionRef, NoData



def get_conncomp(args):
    """
       return a new connected component file that is masked with a new no-data.
       original connected componet has 0 for no-data and also connected component 0
       uses first band of another dataset with 0 as no-data to apply the new no-data masking
    """

    import gdal
    import numpy as np
    import pdb
    import glob

    vrt_file_conn = glob.glob(args[0])[0]
    no_data_conn = args[1]
    vrt_file_aux = glob.glob(args[2])[0]

    print("get_conncomp : vrt_file_conn : {} : vrt_file_aux : {}".format(vrt_file_conn, vrt_file_aux))

    # load connected comp
    conn_comp_data, geoTrans,projectionRef, NoData =  data_loading(vrt_file_conn,out_data_type="float32",data_band=1)
    print("conn_comp_data {}, geoTrans {}, projectionRef {}, NoData {}".format(conn_comp_data, geoTrans,projectionRef, NoData))   
 
    # load the aux file
    aux_data, geoTrans,projectionRef, no_data_aux =  data_loading(vrt_file_aux,out_data_type=None,data_band=1)
    print("aux_data {}, geoTrans {},projectionRef {}, no_data_aux {}".format(aux_data, geoTrans,projectionRef, no_data_aux))
    if no_data_aux is None:
        try: 
            no_data_aux = args[3]
        except:
            no_data_aux = 0.0    

    print("no_data_aux : {}".format(no_data_aux))
    # update the connected comp no-data value
    #ATTENTION!! FAILURE
    #conn_comp_data[aux_data==no_data_aux]=no_data_conn

    # return a dictionary
    output_dict = {}
    output_dict['data'] = conn_comp_data
    output_dict['data_transf'] = geoTrans
    output_dict['data_proj'] = projectionRef
    output_dict['data_nodata'] = no_data_conn
    print(output_dict)
    return output_dict




def get_geocoded_coords_ISCE2(args):
    """Return geo-coordinates center pixel of a GDAL readable file. Note this function is specific for ISCE 2.0 where there is an inconsistency for the pixel definition in the vrt's. Isce assumes center pixel while the coordinate and transf reconstruction required the input to be edge defined."""

    import gdal
    import numpy as np
    import pdb
    import glob

    vrt_file = glob.glob(args[0])[0]
    print("vrt_file : {}".format(vrt_file))

    geovariables = args[1:]

    # extract geo-coded corner coordinates
    ds = gdal.Open(vrt_file)
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    
    # getting the gdal transform and projection
    geoTrans = str(ds.GetGeoTransform())
    projectionRef = str(ds.GetProjection())
    
    count=0
    for geovariable in geovariables:
        variable = geovariable[0]
        if variable == 'longitude' or variable == 'Longitude' or variable == 'lon' or variable == 'Lon':
            lon_arr = list(range(0, cols))
            lons = np.empty((cols,),dtype='float64')
            for px in lon_arr:
                lons[px] = gt[0] + (px * gt[1])
            count+=1
            lons_map = geovariable[1]
        elif variable == 'latitude' or variable == 'Latitude' or variable == 'lat' or variable == 'Lat':
            lat_arr = list(range(0, rows))
            lats = np.empty((rows,),dtype='float64') 
            for py in lat_arr:
                lats[py] = gt[3] + (py * gt[5])
            count+=1
            lats_map = geovariable[1]
        else:
            raise Exception("arguments are either longitude or lattitude")

    # making sure both lon and lat were querried
    if count !=2:
        raise Exception("Did not provide a longitude and latitude argument")

    coordinate_dict = {}
    coordinate_dict['lons'] = lons
    coordinate_dict['lats'] = lats
    coordinate_dict['lons_map'] = lons_map
    coordinate_dict['lats_map'] = lats_map
    coordinate_dict['data_proj'] = projectionRef
    coordinate_dict['data_transf'] = geoTrans
    return coordinate_dict




def get_geocoded_coords(args):
    """Return geo-coordinates center pixel of a GDAL readable file."""

    import gdal
    import numpy as np
    import pdb

    vrt_file = args[0]
    geovariables = args[1:]

    # extract geo-coded corner coordinates
    ds = gdal.Open(vrt_file)
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    
    # getting the gdal transform and projection
    geoTrans = str(ds.GetGeoTransform())
    projectionRef = str(ds.GetProjection())

    count=0
    for geovariable in geovariables:
        variable = geovariable[0]
        if variable == 'longitude' or variable == 'Longitude' or variable == 'lon' or variable == 'Lon':
            lon_arr = list(range(0, cols))
            lons = np.empty((cols,),dtype='float64')
            for px in lon_arr:
                lons[px] = gt[0]+old_div(gt[1],2) + (px * gt[1])
            count+=1
            lons_map = geovariable[1]
        elif variable == 'latitude' or variable == 'Latitude' or variable == 'lat' or variable == 'Lat':
            lat_arr = list(range(0, rows))
            lats = np.empty((rows,),dtype='float64') 
            for py in lat_arr:
                lats[py] = gt[3]-old_div(gt[5],2) + (py * gt[5])
            count+=1
            lats_map = geovariable[1]
        else:
            raise Exception("arguments are either longitude or lattitude")

    # making sure both lon and lat were querried
    if count !=2:
        raise Exception("Did not provide a longitude and latitude argument")

    coordinate_dict = {}
    coordinate_dict['lons'] = lons
    coordinate_dict['lats'] = lats
    coordinate_dict['lons_map'] = lons_map
    coordinate_dict['lats_map'] = lats_map
    coordinate_dict['data_proj'] = projectionRef
    coordinate_dict['data_transf'] = geoTrans
    return coordinate_dict

def get_alos2App_data(alos2app_xml='alos2App_scansar.xml'):
    '''  
        loading the alos2app xml file
    '''

    import isce
    from alos2App import TopsInSAR
    import os
    import pdb
    # prvide the full path and strip off any .xml if pressent
    alos2app_xml = os.path.splitext(os.path.abspath(alos2app_xml))[0]
    curdir = os.getcwd()
    filedir = os.path.dirname(alos2app_xml)
    filename = os.path.basename(alos2app_xml)

    os.chdir(filedir)
    #pdb.set_trace()
    insar = TopsInSAR(name = filename)
    insar.configure()
    os.chdir(curdir)
    return insar

def get_isce_version_info(args):
    import isce
    isce_version = isce.release_version
    if isce.release_svn_revision:
        isce_version = "ISCE version = " + isce_version + ", " + "SVN revision = " + isce.release_svn_revision
    return isce_version



def get_also2_variable2(args):
    get_alos2_variable(args)    

def get_alos2_variable(args):
    '''
        return the value of the requested variable
    '''
    import re
    import os
    import pdb
    import json
    alos2_xml = args[0]
    variable = args[1]
    
    print("\n\nget_alos2_variable : {} : {}".format(alos2_xml, variable))
    #pdb.set_trace()
    #insar = get_alos2App_data(alos2app_xml)
    #print(alos2_xml)
    resp = json.loads(json.dumps(xml_json_converter.xml2json(alos2_xml)))
    #print((json.dumps(resp, indent=2)))
    #print(resp.keys())
    alos2insar = resp['alos2insar']
    #print(alos2insar)
    data = None
    if variable == 'DEM':
        import numpy as np
        print("isce_function: variable = DEM")
        if "dem for coregistration" in alos2insar:
            insar_temp = alos2insar["dem for coregistration"]
            print("isce_function: demFilename Found. insar_temp : %s" %insar_temp)
            if insar_temp.startswith("NED"):
                data = "NED"
            else:
                data = "SRTM"
            
        else:
            print("isce_function : demFilename NOT Found. Defaulting to SRTM")
            data = "SRTM"
    elif variable == 'reference':
        img_file =  os.path.basename(glob.glob(os.path.join(alos2insar['master directory'], "IMG-*"))[0])
        data = re.split("__[A|D]-F*", img_file)[0]
    elif variable == 'secondary':
        img_file =  os.path.basename(glob.glob(os.path.join(alos2insar['slave directory'], "IMG-*"))[0])
        data = re.split("__[A|D]-F*", img_file)[0]

    print("data : {}".format(data))
    return data



def get_alos2_subswath_xml(masterdir):
    ''' 
        Find all available IW[1-3].xml files
    '''

    import os
    import glob
    
    masterdir = os.path.abspath(masterdir)
    IWs = glob.glob(os.path.join(masterdir,'IW*.xml'))
    if len(IWs)<1:
        raise Exception("Could not find a IW*.xml file in " + masterdir)
    return IWs


def get_h5_dataset_coords(args):
    '''
       Getting the coordinates from the meta hdf5 file which is longitude, latitude and height
    '''    
    import pdb
    h5_file = args[0]
    geovariables = args[1:]

    # loop over the variables and track the variable names
    count=0
    for geovariable in geovariables:
        variable = geovariable[0]
        varname = variable.split('/')[-1] 
        if varname == 'longitude' or varname == 'Longitude' or varname == 'lon' or varname == 'Lon' or  varname == 'lons' or varname == 'Lons':
            lons = get_h5_dataset([h5_file,variable])
            count+=1
            lons_map = geovariable[1]
        elif varname == 'latitude' or varname == 'Latitude' or varname == 'lat' or varname == 'Lat' or  varname == 'lats' or varname == 'Lats':
            lats = get_h5_dataset([h5_file,variable])
            count+=1
            lats_map = geovariable[1]
        elif varname == 'height' or varname == 'Height' or varname == 'h' or varname == 'H' or  varname == 'heights' or varname == 'Heights':
            hgts = get_h5_dataset([h5_file,variable])
            count+=1
            hgts_map = geovariable[1]
        else:
            raise Exception("arguments are either longitude, lattitude, or height")

    # making sure both lon and lat were querried
    if count !=3:
        raise Exception("Did not provide a longitude and latitude argument")


    #pdb.set_trace()
    # getting the projection string
    try:
        proj4 = get_h5_dataset([h5_file,"/inputs/projection"])
    except:
        try: 
            proj4 = get_h5_dataset([h5_file,"/projection"])
        except:
            raise Exception

    proj4 = proj4.astype(dtype='str')[0]
    proj4 = int(proj4.split(":")[1])
    from osgeo import osr
    ref = osr.SpatialReference()
    ref.ImportFromEPSG(proj4)
    projectionRef = ref.ExportToWkt()

    coordinate_dict = {}
    coordinate_dict['lons'] = lons.astype(dtype='float64')
    coordinate_dict['lats'] = lats.astype(dtype='float64') 
    coordinate_dict['hgts'] = hgts.astype(dtype='float64') 
    coordinate_dict['lons_map'] = lons_map
    coordinate_dict['lats_map'] = lats_map
    coordinate_dict['hgts_map'] = hgts_map
    coordinate_dict['data_proj'] = projectionRef
    return coordinate_dict



def get_h5_dataset(args):
    ''' 
        Extracts a hdf5 variable and return the content of it
        INPUTS:
        filename    str of the hdf5 file
        variable    str describing the path within the hdf5 file: e.g. cube/dataset1
    '''

    import h5py    
    import numpy as np    
   
    file_name=  args[0]
    path_variable = args[1]
    datafile = h5py.File(file_name,'r') 
    data = datafile[path_variable].value

    return data

def check_file_exist(infile):
    import os
    if not os.path.isfile(infile):
        raise Exception(infile + " does not exist")

def read_isce_product(xmlfile):
    import os
    import isce 
    from iscesys.Component.ProductManager import ProductManager as PM

    # check if the file does exist
    check_file_exist(xmlfile)

    # loading the xml file with isce
    pm = PM()
    pm.configure()
    obj = pm.loadProduct(xmlfile)

    return obj

def get_orbit():
    from isceobj.Orbit.Orbit import Orbit

    """Return orbit object."""

    orb = Orbit()
    orb.configure()
    return orb

def get_aligned_bbox(prod, orb):
    """Return estimate of 4 corner coordinates of the
       track-aligned bbox of the product."""
    import gdal
    import numpy as np
    import os

    # create merged orbit
    burst = prod.bursts[0]

    #Add first burst orbit to begin with
    for sv in burst.orbit:
         orb.addStateVector(sv)

    ##Add all state vectors
    for bb in prod.bursts:
        for sv in bb.orbit:
            if (sv.time< orb.minTime) or (sv.time > orb.maxTime):
                orb.addStateVector(sv)
        bb.orbit = orb

    # extract bbox
    ts = [prod.sensingStart, prod.sensingStop]
    rngs = [prod.startingRange, prod.farRange]
    pos = []
    for tim in ts:
        for rng in rngs:
            llh = prod.orbit.rdr2geo(tim, rng, height=0.)
            pos.append(llh)
    pos = np.array(pos)
    bbox = pos[[0, 1, 3, 2], 0:2]
    return bbox.tolist()

def get_loc(box):
    """Return GeoJSON bbox."""
    import numpy as np
    import os

    bbox = np.array(box).astype(np.float)
    coords = [
        [ bbox[0,1], bbox[0,0] ],
        [ bbox[1,1], bbox[1,0] ],
        [ bbox[2,1], bbox[2,0] ],
        [ bbox[3,1], bbox[3,0] ],
        [ bbox[0,1], bbox[0,0] ],
    ]
    return {
        "type": "Polygon",
        "coordinates":  [coords] 
    }

def get_env_box(env):

    #print("get_env_box env " %env)
    bbox = [
        [ env[3], env[0] ],
        [ env[3], env[1] ],
        [ env[2], env[1] ],
        [ env[2], env[0] ],
    ]
    print("get_env_box box : %s" %bbox)
    return bbox


def get_union_geom(bbox_list):
    from osgeo import gdal, ogr, osr
    import json

    geom_union = None
    for bbox in bbox_list:
        loc = get_loc(bbox)
        geom = ogr.CreateGeometryFromJson(json.dumps(loc))
        print("get_union_geom : geom : %s" %get_union_geom)
        if geom_union is None:
            geom_union = geom
        else:
            geom_union = geom_union.Union(geom)
    print("geom_union_type : %s" %type(geom_union)) 
    return geom_union

def get_area(coords):
    '''get area of enclosed coordinates- determines clockwise or counterclockwise order'''
    n = len(coords) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += coords[i][1] * coords[j][0]
        area -= coords[j][1] * coords[i][0]
    #area = abs(area) / 2.0
    return old_div(area, 2)

def change_direction(coords):
    cord_area= get_area(coords)
    if not get_area(coords) > 0: #reverse order if not clockwise
        print("update_met_json, reversing the coords")
        coords = coords[::-1]
    return coords

def get_raster_corner_coords(vrt_file):
    """Return raster corner coordinates."""
    import gdal
    import os

    # go to directory where vrt exists to extract from image
    cwd =os.getcwd()
    data_dir = os.path.dirname(os.path.abspath(vrt_file))
    os.chdir(data_dir)

    # extract geo-coded corner coordinates
    ds = gdal.Open(os.path.basename(vrt_file))
    gt = ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize
    ext = []
    lon_arr = [0, cols]
    lat_arr = [0, rows]
    for px in lon_arr:
        for py in lat_arr:
            lon = gt[0] + (px * gt[1]) + (py * gt[2])
            lat = gt[3] + (px * gt[4]) + (py * gt[5])
            ext.append([lat, lon])
        lat_arr.reverse()
    os.chdir(cwd)
    return ext


def get_bbox(args):
    import json
    import os
    import ogr
    import pdb

    cur_dir = os.path.dirname(os.path.abspath(__file__))
    cur_wd = os.getcwd()
    master_dir= args[0]

    print("isce_functions : get_bbox: %s : %s : %s" %(cur_dir, cur_wd, master_dir))
    bboxes = []
    master_dir = args[0]

    IWs = get_alos2_subswath_xml(master_dir)
    print("isce_functions : get_bbox : after get_alos2_subswath_xml : %s" %len(IWs))
    for IW in IWs:
        try:
            prod = read_isce_product(IW)
            print("isce_functions: after prod")
            orb = get_orbit()
            print("isce_functions : orb")
            bbox_swath = get_aligned_bbox(prod, orb)
            print("isce_functions : bbox_swath : %s" %bbox_swath)
            bboxes.append(bbox_swath)
        except Exception as e:
            print("isce_functions : Failed to get aligned bbox: %s" %str(e))
            #print("Getting raster corner coords instead.")
            #bbox_swath = get_raster_corner_coords(vrt_file)

    geom_union = get_union_geom(bboxes)
    print("isce_functions : geom_union : %s" %geom_union)
    # return the polygon as a list of strings, which each poly a list argument
    geom_union_str = ["%s"%geom_union]
    return geom_union_str


def create_alos2app_xml(dir_name):
    fp = open('alos2App.xml', 'w')
    fp.write('<alos2App>\n')
    fp.write('    <component name="alos2insar">\n')
    fp.write('        <property name="master directory">{}</property>\n'.format(os.path.abspath(dir_name)))
    fp.write('        <property name="slave directory">{}</property>\n'.format(os.path.abspath(dir_name)))
    fp.write('    </component>\n')
    fp.write('</alos2App>\n')
    fp.close()


