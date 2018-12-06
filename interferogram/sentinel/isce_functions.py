#!/usr/bin/env python3
# David Bekaert - Jet Propulsion Laboratory
# set of functions that are leveraged in the packaging of the ARIA standard product 

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

    vrt_file_conn = args[0]
    no_data_conn = args[1]
    vrt_file_aux = args[2]

    # load connected comp
    conn_comp_data, geoTrans,projectionRef, NoData =  data_loading(vrt_file_conn,out_data_type="float32",data_band=1)
    
    # load the aux file
    aux_data, geoTrans,projectionRef, no_data_aux =  data_loading(vrt_file_aux,out_data_type=None,data_band=1)
    if no_data_aux is None:
        try: 
            no_data_aux = args[3]
        except:
            no_data_aux = 0.0    

    # update the connected comp no-data value
    conn_comp_data[aux_data==no_data_aux]=no_data_conn

    # return a dictionary
    output_dict = {}
    output_dict['data'] = conn_comp_data
    output_dict['data_transf'] = geoTrans
    output_dict['data_proj'] = projectionRef
    output_dict['data_nodata'] = no_data_conn
    return output_dict




def get_geocoded_coords_ISCE2(args):
    """Return geo-coordinates center pixel of a GDAL readable file. Note this function is specific for ISCE 2.0 where there is an inconsistency for the pixel definition in the vrt's. Isce assumes center pixel while the coordinate and transf reconstruction required the input to be edge defined."""

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
                lons[px] = gt[0]+gt[1]/2 + (px * gt[1])
            count+=1
            lons_map = geovariable[1]
        elif variable == 'latitude' or variable == 'Latitude' or variable == 'lat' or variable == 'Lat':
            lat_arr = list(range(0, rows))
            lats = np.empty((rows,),dtype='float64') 
            for py in lat_arr:
                lats[py] = gt[3]-gt[5]/2 + (py * gt[5])
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

def get_topsApp_data(topsapp_xml='topsApp'):
    '''  
        loading the topsapp xml file
    '''

    import isce
    from topsApp import TopsInSAR
    import os
    import pdb
    # prvide the full path and strip off any .xml if pressent
    topsapp_xml = os.path.splitext(os.path.abspath(topsapp_xml))[0]
    curdir = os.getcwd()
    filedir = os.path.dirname(topsapp_xml)
    filename = os.path.basename(topsapp_xml)

    os.chdir(filedir)
    #pdb.set_trace()
    insar = TopsInSAR(name = filename)
    insar.configure()
    os.chdir(curdir)
    return insar

def get_topsApp_variable(args):
    '''
        return the value of the requested variable
    '''
   
    import os
    import pdb
    topsapp_xml = args[0]
    variable = args[1]

    #pdb.set_trace()
    insar = get_topsApp_data(topsapp_xml)
    # ESD specific
    if variable == 'ESD':
        import numpy as np
        if insar.__getattribute__('doESD'):
            insar_temp = insar.__getattribute__('esdCoherenceThreshold')
        else:
            insar_temp = -1.0 
        data = np.float(insar_temp)
    # other variables
    elif variable == 'DEM':
        import numpy as np
        print("isce_function: variable = DEM")
        if insar.__getattribute__('demFilename'):
            insar_temp = insar.__getattribute__('demFilename')
            print("isce_function: demFilename Found. insar_temp : %s" %insar_temp)
            if insar_temp.startswith("NED"):
                data = "NED"
            else:
                data = "SRTM"
            
        else:
            print("isce_function : demFilename NOT Found. Defaulting to SRTM")
            data = "SRTM"
    else:
        # tops has issues with calling a nested variable, will need to loop over it
        variables = variable.split('.')
        insar_temp = insar
        for variable in variables:
            insar_temp = insar_temp.__getattribute__(variable)
        data = insar_temp

    # further processing if needed
    # removing any paths and only re-ruturning a list of files
    if variable == 'safe':
        data  = [os.path.basename(SAFE) for SAFE in data]

    return data



def get_tops_subswath_xml(masterdir):
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


def get_tops_metadata_variable(args):
    '''
        return the value of the requested variable
    '''
    masterdir = args[0]
    variable = args[1]
    tops_metadata = get_tops_metadata(masterdir)
    data = tops_metadata[variable]

    return data

def get_tops_metadata(masterdir):
    import pdb
    from scipy.constants import c

    # get a list of avialble xml files for IW*.xml
    IWs = get_tops_subswath_xml(masterdir)

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
    output['frequency']  = c/output['wavelength']
    if "POEORB" in obj.orbit.getOrbitSource():
        output['orbittype'] = "precise"
    elif "RESORB" in obj.orbit.getOrbitSource():
        output['orbittype'] = "restituted"
    else:
        output['orbittype'] = ""
    # geo transform grt for x y 
    # bandwith changes per swath - placeholder c*b/2 or delete
    # tops xml file
    # refer to safe files frame doppler centroid burst[middle].doppler
    # extract from topsapp.xml 

    return output



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
