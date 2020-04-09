#!/usr/bin/env python3

from __future__ import division
from builtins import str
from builtins import range
from builtins import object
from past.utils import old_div
import isce
from isceobj.Scene.Frame import Frame
from isceobj.Planet.AstronomicalHandbook import Const
from isceobj.Planet.Planet import Planet
from Sentinel1_TOPS import Sentinel1_TOPS
import argparse
from lxml import objectify as OBJ
from FrameInfoExtractor import FrameInfoExtractor as FIE
import numpy as np
from osgeo import ogr, osr
import os, sys, re, requests, json, shutil, traceback, logging, hashlib, math

DATASETTYPE_RE = re.compile(r'-(raw|slc)-')

MISSION_RE = re.compile(r'S1(\w)')


def cmdLineParse():
    '''
    Command line parsing.
    '''

    parser = argparse.ArgumentParser(description='Extract metadata from S1 swath')
    #parser.add_argument('-i','--input', dest='inxml', type=str, required=True,
            #help='Swath XML file')a
    parser.add_argument('-i','--input', dest='xml_file', type=str, nargs='+', help='Swath XML file')
    parser.add_argument('-o', '--output', dest='outjson', type=str, required=True,
            help = 'Ouput met.json')
    return parser.parse_args()

def objectify(inxml):
    '''
    Return objectified XML.
    '''
    with open(inxml, 'r') as fid:
        root = OBJ.parse(fid).getroot()
    return root

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

def getGeometry(obj):
    '''
    Get bbox and central coordinates.
    '''
    pts = []
    glist = obj.geolocationGrid.geolocationGridPointList

    for child in glist.getchildren():
        pts.append( [float(child.line), float(child.pixel), float(child.latitude), float(child.longitude)])

    ys = sorted(list(set([x[0] for x in pts])))
    dy = ys[1] - ys[0]
    ny= int(old_div((ys[-1] - ys[0]),dy) + 1)

    xs = sorted(list(set([x[1] for x in pts])))
    dx = xs[1] - xs[0]
    nx = int(old_div((xs[-1] - xs[0]),dx) + 1)

    lat = np.array([x[2] for x in pts]).reshape((ny,nx))
    lon = np.array([x[3] for x in pts]).reshape((ny,nx))

    bbox = [[lat[0,0],lon[0,0]], [lat[0,-1],lon[0,-1]],
            [lat[-1,-1],lon[-1,-1]], [lat[-1,0], lon[-1,0]]]

    center = { "coordinates": [lon[ny//2,nx//2], lat[ny//2, nx//2]],
               "type" : "point"}

    return center, bbox


class S1toFrame(object):
    '''
    Create a traditional ISCE Frame object from S1 container.
    '''

    def __init__(self, sar, obj):
        self.sar = sar
        self.obj = obj
        self.missionId = self.obj.xpath('.//missionId/text()')[0]
        self.missionId_char = MISSION_RE.search(self.missionId).group(1)
        self.frame = Frame()
        self.frame.configure()

        self.parse()

    def parse(self):
        self._populatePlatform()
        self._populateInstrument()
        self._populateFrame()
        self._populateOrbit()
        self._populateExtras()

    def _populatePlatform(self):
        platform = self.frame.getInstrument().getPlatform()
        platform.setMission(self.missionId)
        platform.setPlanet(Planet(pname='Earth'))
        platform.setPointingDirection(-1)
        platform.setAntennaLength(40.0)
        
    def _populateInstrument(self):
        ins = self.frame.getInstrument()
        b0 = self.sar.bursts[0]
        b1 = self.sar.bursts[-1]

        ins.setRadarWavelength(b0.radarWavelength)
        ins.setPulseRepetitionFrequency(1.0/b0.azimuthTimeInterval)
        ins.setRangePixelSize(b0.rangePixelSize)

        tau = self.obj.generalAnnotation.replicaInformationList.replicaInformation.referenceReplica.timeDelay
        ins.setPulseLength(float(tau))
        slope = str(self.obj.generalAnnotation.replicaInformationList.replicaInformation.referenceReplica.phaseCoefficients).split()[2]
        ins.setChirpSlope(float(slope))
        
        fsamp = old_div(Const.c, (2.0 * b0.rangePixelSize))
        ins.setRangeSamplingRate(fsamp)

        ins.setInPhaseValue(127.5)
        ins.setQuadratureValue(127.5)
        ins.setBeamNumber(self.obj.adsHeader.swath)
       
    def _populateFrame(self):
        frame = self.frame
        b0 = self.sar.bursts[0]
        b1 = self.sar.bursts[-1]


        hdg = self.obj.generalAnnotation.productInformation.platformHeading
        if hdg < -90:
            frame.setPassDirection('Descending')
        else:
            frame.setPassDirection('Ascending')

        frame.setStartingRange(b0.startingRange)
        frame.setOrbitNumber(int(self.obj.adsHeader.absoluteOrbitNumber))
        frame.setProcessingFacility('Sentinel 1%s' % self.missionId_char)
        frame.setProcessingSoftwareVersion('IPF')
        frame.setPolarization(self.obj.adsHeader.polarisation)
        frame.setNumberOfSamples(int(self.obj.imageAnnotation.imageInformation.numberOfSamples))
        frame.setNumberOfLines(int(self.obj.imageAnnotation.imageInformation.numberOfLines))
        frame.setSensingStart(b0.sensingStart)
        frame.setSensingStop(b1.sensingStop)

        tmid = b0.sensingStart + 0.5 * (b1.sensingStop - b0.sensingStart)
        frame.setSensingMid(tmid)
        
        farRange = b0.startingRange + frame.getNumberOfSamples() * b0.rangePixelSize
        frame.setFarRange(farRange)

    def _populateOrbit(self):
        b0 = self.sar.bursts[0]
        self.frame.orbit = b0.orbit


    def _populateExtras(self):
        b0 = self.sar.bursts[0]
        self.frame._squintAngle = 0.0
        self.frame.doppler = b0.doppler._coeffs[0]
        match = DATASETTYPE_RE.search(self.sar.xml)
        if match: self.frame.datasetType = 'slc'
        else: self.frame.datasetType = ''


def get_loc(frameInfo, bbox_type):
    """Return GeoJSON bbox."""

    bbox = np.array(frameInfo.getBBox()).astype(np.float)
    print("get_loc bbox: %s" %bbox)
    if bbox_type == "refbbox":
        bbox = np.array(frameInfo.getReferenceBBox()).astype(np.float)
    coords = [
        [ bbox[0,1], bbox[0,0] ],
        [ bbox[1,1], bbox[1,0] ],
        [ bbox[2,1], bbox[2,0] ],
        [ bbox[3,1], bbox[3,0] ],
        [ bbox[0,1], bbox[0,0] ],
    ]

    print("get_loc coords : [%s]" %coords)
    return {
        "type": "Polygon",
        "coordinates":  [coords] 
    }

def set_value(param, value):
    try:
        param = value
        print("set value of %s is %s" %(param, value))
    except Exception as e:
        print(traceback.format_exc())


def get_union_geom(frame_infoes, bbox_type):
    geom_union = None
    for frameInfo in frame_infoes:
        loc = get_loc(frameInfo, bbox_type)

        print("get_union_geom loc : %s" %loc)
        geom = ogr.CreateGeometryFromJson(json.dumps(loc))
        print("get_union_geom : geom : %s" %get_union_geom)
        if geom_union is None:
            geom_union = geom
        else:
            geom_union = geom_union.Union(geom)
        print("union geom : %s " %geom_union)
    print("final geom_union : %s" %geom_union)
    print("extract data geom_union type : %s" %type(geom_union))
    return geom_union


def get_env_box(env):

    #print("get_env_box env :%s" %env)
    bbox = [
        [ env[3], env[0] ],
        [ env[3], env[1] ],
        [ env[2], env[1] ],
        [ env[2], env[0] ],
    ]
    print("get_env_box box : %s" %bbox)
    return bbox

def create_stitched_met_json( frame_infoes, met_json_file):
    """Create HySDS met json file."""

    # build met

    geom_union = get_union_geom(frame_infoes, "bbox")
    print("create_stitched_met_json : bbox geom_union : %s" %geom_union)
    bbox = json.loads(geom_union.ExportToJson())["coordinates"][0]
    print("create_stitched_met_json : bbox : %s" %bbox)
    bbox = get_env_box(geom_union.GetEnvelope())
    bbox = change_direction(bbox)
    print("create_stitched_met_json :Final bbox : %s" %bbox)

    geom_union = get_union_geom(frame_infoes, "refbbox")
    print("create_stitched_met_json : refbbox geom_union : %s" %geom_union)
    refbbox = json.loads(geom_union.ExportToJson())["coordinates"][0]
    print("create_stitched_met_json : refbbox : %s" %refbbox)
    refbbox = get_env_box(geom_union.GetEnvelope())
    refbbox = change_direction(refbbox)
    print("create_stitched_met_json :Final refbbox : %s" %refbbox)

    #refbbox = json.loads(get_union_geom(frame_infoes, "refbbox").ExportToJson())["coordinates"][0]
    #print("create_stitched_met_json : refbbox : %s" %refbbox)
    met = {
        'product_type': 'interferogram',
        #'master_scenes': [],
        'refbbox': refbbox,
        #'esd_threshold': [],
        'frameID': [],
        #'temporal_span': [],
        #'swath': [1, 2, 3],
        'trackNumber': [],
        #'archive_filename': id,
        'dataset_type': 'slc',
        'tile_layers': [],
        #'latitudeIndexMin': int(math.floor(env[2] * 10)),
        #'latitudeIndexMax': int(math.ceil(env[3] * 10)),
        'latitudeIndexMin': [],
        'latitudeIndexMax': [],
        #'parallelBaseline': [],
        'url': [],
        'doppler': [],
        #'version': [],
        #'slave_scenes': [],
        #'orbit_type': [],
        #'spacecraftName': [],
        'frameNumber': None,
        #'reference': None,
        'bbox': bbox,
        'ogr_bbox': [],
        'orbitNumber': [],
        #'inputFile': 'sentinel.ini',
        #'perpendicularBaseline': [],
        'orbitRepeat': [],
        'sensingStop': [],
        #'polarization': [],
        #'scene_count': 0,
        'beamID': None,
        'sensor': [],
        'lookDirection': [],
        'platform': [],
        'startingRange': [],
        'frameName': [],
        #'tiles': True,
        'sensingStart': [],
        #'beamMode': [],
        #'imageCorners': [],
        'direction': [],
        'prf': [],
        #'range_looks': [],
        #'dem_type': None,
        #'filter_strength': [],
	#'azimuth_looks': [],
        "sha224sum": hashlib.sha224(str.encode(os.path.basename(met_json_file))).hexdigest()
    }

    
    # collect values

    set_params=('tile_layers', 
                'latitudeIndexMin',  'url', 'prf', 'doppler', 'platform', 'orbitNumber',
                'latitudeIndexMax', 'sensingStop', 'startingRange', 'sensingStart'
		#'master_scenes', 'temporal_span', 'swath'
               )

    single_params = ('frameID', 'sensor', 'beamID', 'frameNumber', 'trackNumber',
                      'dataset_type',  'archive_filename',
                     'direction', 'orbitRepeat', 'lookDirection','frameName', 'product_type'
                    #,'esd_threshold'
                    )
    list_params=( 'tile_layers', 'latitudeIndexMin',  'url', 'prf', 'doppler', 'platform', 'orbitNumber',
                'latitudeIndexMax', 'sensingStop', 'startingRange', 'sensingStart'
                #'master_scenes', temporal_span' , 'swath'
                 )

    mean_params = ( 'prf', 'doppler')

    min_params = ('latitudeIndexMin', 'startingRange', 'sensingStart' )
    max_params = ('latitudeIndexMax', 'sensingStop')
    

    for i, frame_info in enumerate(frame_infoes):
        md = frame_info.toDict()
        
        for param in set_params:
            if param not in md:
                continue
            print(" set param: {}".format(param))
            if isinstance(md[param], list):
                met[param].extend(md[param])
            else:
                met[param].append(md[param])
        if i == 0:
            for param in single_params:
                if param in md:
                    met[param] = md[param]
        ##met['scene_count'] += 1
    for param in set_params:
        print("param: {}".format(param))
        tmp_met = list(set(met[param]))
        if param in list_params:
            met[param] = tmp_met
        else:
            met[param] = tmp_met[0] if len(tmp_met) == 1 else tmp_met
    for param in mean_params:
        print("mean param: %s type : %s " %(param, type(param)))
        met[param] = np.mean(met[param])
    for param in min_params:
        print("min param: %s type : %s " %(param, type(param)))
        if met[param] is None:
            print("Missing Min Param : %s" %param)
        else:
            print(met[param])
            met[param] = min(met[param])
    for param in max_params:
        print("max param: %s type : %s " %(param, type(param)))
        if met[param] is None:
            print("Missing Max Param : %s" %param)
        else:
            print(met[param])
            met[param] = max(met[param])
   
 
    #met['imageCorners'] = get_image_corners(met['imageCorners'])
    try:
        print(bbox)
        print(type(bbox))
        met['ogr_bbox'] = [[x, y] for y, x in bbox]
    except Exception as e:
        print(traceback.format_exc())
	
    # write out dataset json
    with open(met_json_file, 'w') as f:
        json.dump(met, f, indent=2)


if __name__ == '__main__':
    '''
    Main driver.
    '''
    
    #Parse command line
    inps = cmdLineParse()

    #Read in metadata
    xml_files=inps.xml_file
    frame_infos=[]
    i=0
    for inxml in xml_files:
        i=i+1
        sar = Sentinel1_TOPS()
        met_file= "test_met%s.json"%i
        sar.xml = inxml
        print("Extract Metadata : Processing %s" %inxml)
        sar.parse()
        obj = objectify(inxml)
    
        ####Copy into ISCE Frame
        frame = S1toFrame(sar,obj)

        ####Frameinfoextractor
        fie = FIE()
        frameInfo = fie.extractInfoFromFrame(frame.frame)
        print("printing FramInfo :\n")
        print(frameInfo)
        frame_infos.append(frameInfo)
        frameInfo.dump(met_file)

    create_stitched_met_json(  frame_infos, inps.outjson)
