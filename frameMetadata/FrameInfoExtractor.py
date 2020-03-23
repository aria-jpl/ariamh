#! /usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


import os
from mroipac.geolocate.Geolocate import Geolocate
import logging
import logging.config
logging.config.fileConfig(
    os.path.join(os.environ['ISCE_HOME'], 'defaults', 'logging',
        'logging.conf')
)
import math
import sys
import numpy
import json
from httplib2 import Http
from urllib.parse import urlencode
from frameMetadata.FrameMetadata import FrameMetadata
from frameMetadata.OrbitInfo import OrbitInfo
from utils.UrlUtils import UrlUtils
from utils.queryBuilder import postQuery,buildQuery,createMetaObjects
import math

# at the moment there is no localized config with the grq server. get it from the PegRegionChecker
# to avoid having it in too many places
class FrameInfoExtractor():

    STATUS_QUERY_OK = '200'
    _latitudeResolution = .1

    def __init__(self):
        self.logger = logging.getLogger("FrameInfoExtractor")
        self._frameFilename = ''
        self._frame = None 
        self._lookDirectionMap = {-1:'right', 1:'left'}
        self._latitudeResolution = .1
        self._buffer = .1

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['logger']
        return d
    def __setstate__(self, d):
        self.__dict__.update(d)
        self.logger = logging.getLogger("FrameInfoExtractor")
    
    def setFrameFilename(self, name):
        self._frameFilename = name
    
    def calculateCorners(self):
        """
        Calculate the approximate geographic coordinates of corners of the SAR image.

        @return (\a tuple) a list with the corner coordinates and a list with the look angles to these coordinates
        """
        # Extract the planet from the hh object
        
        planet = self._frame.getInstrument().getPlatform().getPlanet()
        # Wire up the geolocation object
        geolocate = Geolocate()
        geolocate.wireInputPort(name='planet', object=planet)
        
        earlySquint = self._frame._squintAngle 
        # Get the ranges, squints and state vectors that defined the boundaries of the frame
        orbit = self._frame.getOrbit()               
        nearRange = self._frame.getStartingRange()
        farRange = self._frame.getFarRange()        
        earlyStateVector = orbit.interpolateOrbit(self._frame.getSensingStart())
        lateStateVector = orbit.interpolateOrbit(self._frame.getSensingStop())            
        nearEarlyCorner, nearEarlyLookAngle, nearEarlyIncAngle = geolocate.geolocate(earlyStateVector.getPosition(),
                                                                                   earlyStateVector.getVelocity(),
                                                                                   nearRange, earlySquint)        
        farEarlyCorner, farEarlyLookAngle, farEarlyIncAngle = geolocate.geolocate(earlyStateVector.getPosition(),
                                                                                earlyStateVector.getVelocity(),
                                                                                farRange, earlySquint)
        nearLateCorner, nearLateLookAngle, nearLateIncAngle = geolocate.geolocate(lateStateVector.getPosition(),
                                                                                lateStateVector.getVelocity(),
                                                                                nearRange, earlySquint)
        farLateCorner, farLateLookAngle, farLateIncAngle = geolocate.geolocate(lateStateVector.getPosition(),
                                                                             lateStateVector.getVelocity(),
                                                                             farRange, earlySquint)
        self.logger.debug("Near Early Corner: %s" % nearEarlyCorner)
        self.logger.debug("Near Early Look Angle: %s" % nearEarlyLookAngle)
        self.logger.debug("Near Early Incidence Angle: %s " % nearEarlyIncAngle)

        self.logger.debug("Far Early Corner: %s" % farEarlyCorner)
        self.logger.debug("Far Early Look Angle: %s" % farEarlyLookAngle)
        self.logger.debug("Far Early Incidence Angle: %s" % farEarlyIncAngle)

        self.logger.debug("Near Late Corner: %s" % nearLateCorner)
        self.logger.debug("Near Late Look Angle: %s" % nearLateLookAngle)
        self.logger.debug("Near Late Incidence Angle: %s" % nearLateIncAngle)

        self.logger.debug("Far Late Corner: %s" % farLateCorner)
        self.logger.debug("Far Late Look Angle: %s" % farLateLookAngle)
        self.logger.debug("Far Late Incidence Angle: %s" % farLateIncAngle)

        corners = [nearEarlyCorner, farEarlyCorner, nearLateCorner, farLateCorner]
        lookAngles = [nearEarlyLookAngle, farEarlyLookAngle, nearLateLookAngle, farLateLookAngle]
        return corners, lookAngles


    def extractInfoFromFrame(self, frame):
        self._frame = frame
        return self.extractInfo()

    # update the frame by setting the attribute attr to the value val. if obj is a string then assume that is a filename, otherwise assume that is a frame object
    def updateFrameInfo(self, attr, val, obj):
        from isceobj.Scene import Frame
        if(isinstance(obj, str)):
            import pickle as cP
            fp = open(obj, 'r')
            frame = cP.load(fp)
            fp.close()
            if(isinstance(attr, list)):
                for i in range(len(attr)):
                    setattr(frame, attr[i], val[i])
            else:
                setattr(frame, attr, val)
            # update the pickled file
            fp = open(obj, 'w')
            cP.dump(frame, fp, 2)
            fp.close()

        elif(isinstance(obj, Frame)):
            frame = obj
            if(isinstance(attr, list)):
                for i in range(len(attr)):
                    setattr(frame, attr[i], val[i])
            else:
                setattr(frame, attr, val)
        else:
            self.logger.error("Error. The method updateFrameInfo takes as third argument a string or a Frame object.")
            raise Exception
    def extractTrack(self, fm):
        if(fm.spacecraftName.lower() == 'alos'):
            fm._trackNumber = (46 * int(fm._orbitNumber) + 84) % 671 + 1
        elif(fm.spacecraftName.lower() == 'csks1' or fm.spacecraftName.lower() == 'csks2'
             or fm.spacecraftName.lower() == 'csks3'):
            fm._trackNumber = fm._orbitNumber % 237
        elif(fm.spacecraftName.lower() == 'csks4'):
            fm._trackNumber = (fm._orbitNumber - 193) % 237
        elif(fm.spacecraftName.lower() == 's1a'):
            # per https://scihub.copernicus.eu/news/News00014
            fm._trackNumber = (fm._orbitNumber - 73) % 175 + 1
        elif(fm.spacecraftName.lower() == 's1b'):
            fm._trackNumber = (fm._orbitNumber - 27) % 175 + 1
        elif(fm.spacecraftName.lower() == 'alos2'):
            fm._trackNumber = (14*fm._orbitNumber+24) % 207
        else:
            print('Unsupported spacecraft',fm.spacecraftName)
            raise Exception
        

        # for envi need to get it from the filename, so need to add it to metadata  
    def extractOrbitRepeat(self, fm):
        if(fm.spacecraftName.lower() == 'alos'):
            fm.orbitRepeat = 671
        elif(fm.spacecraftName.lower().count('csk')):
            fm.orbitRepeat = 237
        elif(fm.spacecraftName.lower().count('s1a')):
            fm.orbitRepeat = 175
        elif(fm.spacecraftName.lower().count('s1b')):
            fm.orbitRepeat = 175
        elif(fm.spacecraftName.lower() == 'alos2'):
            fm.orbitRepeat = 207
        else:
            print('Unsupported spacecraft',fm.spacecraftName)
            raise Exception
            
        # envi is 431
    def extractPlatform(self, fm):
        if(fm.spacecraftName.lower() == 'alos'):
            fm.platform = 'alos'
        elif(fm.spacecraftName.lower().count('csk')):
            fm.platform = 'csk'
        elif(fm.spacecraftName.lower().count('s1a')):
            fm.platform = 's1a'
        elif(fm.spacecraftName.lower().count('s1b')):
            fm.platform = 's1b'
        elif(fm.spacecraftName.lower() == 'alos2'):
            fm.platform = 'alos2'
        else:
            print('Unsupported spacecraft',fm.spacecraftName)
            raise Exception
            
        # envi is 431
    def extractPlatform(self, fm):
        if(fm.spacecraftName.lower() == 'alos'):
            fm.platform = 'alos'
        elif(fm.spacecraftName.lower().count('csk')):
            fm.platform = 'csk'
        elif(fm.spacecraftName.lower().count('s1a')):
            fm.platform = 's1a'
        elif(fm.spacecraftName.lower().count('s1b')):
            fm.platform = 's1b'
        elif(fm.spacecraftName.lower() == 'alos2'):
            fm.platform = 'alos2'
        else:
            print('Unsupported spacecraft',fm.spacecraftName)
            raise Exception
      
    def computeFrameID(self,fm):
        fm.frameID = int(round((fm.latitudeIndexMin + fm.latitudeIndexMax)/2))
           
    def computeLatitudeIndeces(self, fm):
        extremes = self.getExtremes(fm.refbbox)
        latMin = extremes[0]
        latMax = extremes[1]
        fm.latitudeIndexMin = int(math.floor(latMin / self._latitudeResolution))
        fm.latitudeIndexMax = int(math.ceil(latMax / self._latitudeResolution))


    # given a bbox return the list [latMin,latMax,lonMin,lonMax]
    def getExtremes(self, bbox):
        return [min(min(min(bbox[0][0], bbox[1][0]), bbox[2][0]), bbox[3][0]),
                max(max(max(bbox[0][0], bbox[1][0]), bbox[2][0]), bbox[3][0]),
                min(min(min(bbox[0][1], bbox[1][1]), bbox[2][1]), bbox[3][1]),
                max(max(max(bbox[0][1], bbox[1][1]), bbox[2][1]), bbox[3][1])]


# return True/False depending if it failed or not

    def computeBaseline(self, fm):
       
        ret = True
        oi = OrbitInfo(fm)
        requester = Http()
        uu = UrlUtils()
        rest_url = uu.rest_url
        
        fmRef = FrameMetadata()
        # just need an estimate
        bbox , dummy = self.calculateCorners()
        fm._bbox = []
        fm._refbbox = []
        for bb in bbox:
            fm._bbox.append([round(bb.getLatitude(), 2), round(bb.getLongitude(), 2)])
            fm._refbbox.append([round(bb.getLatitude(), 2), round(bb.getLongitude(), 2)])
        if(fm._bbox[0][0] < fm._bbox[2][0]):
            # if latEarly < latLate then asc otherwise dsc
            fm._direction = 'asc'
        else:
            fm._direction = 'dsc'

       
        baseline = [0, 0, 0]
        uu = UrlUtils()
        extremes = fm.getExtremes(fm.bbox)
        latMin = extremes[0]
        latMax = extremes[1]
        latDelta = (latMax - latMin) / 3.
        latitudeResolution = .1
        params = {
            'sensor': fm.platform,
            'trackNumber':fm.trackNumber,
            'dataset_type':fm.dataset_type,
            'latitudeIndexMin': int(math.floor((latMin - latDelta)/latitudeResolution)),
            'latitudeIndexMax': int(math.ceil((latMax + latDelta)/latitudeResolution)),
            'direction':fm.direction,
            'system_version':uu.version,
            'lookDirection':fm.lookDirection,
            'reference':True
            }
        if fm.beamID:
            params['beamID'] =  fm.beamID
        #print("params", params)
        query = buildQuery(params,['within'])
        #print("query: %s" % json.dumps(query, indent=2))
        metList,status = postQuery(query)
                
        # if empty no results available
        if status:
            metObj = createMetaObjects(metList)
            if metObj:
                # there should be only one result
                if(len(metObj) > 1):
                    print("WARNING FrameInfoExtractor: Expecting only one frame to be reference")
                
                fmRef = metObj[0]
                oiRef = OrbitInfo(fmRef)
                oi.computeBaseline(oiRef)
                bl = oi.getBaseline()
                baseline = [bl['horz'], bl['vert'], bl['total']]
                fm.refbbox = fmRef.refbbox
                fm.reference = False
                fm._bbox = []
                for bb in bbox:
                    fm._bbox.append([round(bb.getLatitude(), 2), round(bb.getLongitude(), 2)])
                if(fm._bbox[0][0] < fm._bbox[2][0]):
                    # if latEarly < latLate then asc otherwise dsc
                    fm._direction = 'asc'
                else:
                    fm._direction = 'dsc'
            else:
                import numpy as np
                fm.reference = True
                pos = np.array(fm._bbox)
                d10 = pos[1] - pos[0]
                d30 = pos[3] - pos[0]
                d23 = pos[2] - pos[3]
                d21 = pos[2] - pos[1]
                pos[0] += self._buffer * (-d10 - d30)
                pos[1] += self._buffer * (d10 - d21)
                pos[2] += self._buffer * (d23 + d21)
                pos[3] += self._buffer * (-d23 + d30)
                fm._refbbox = pos.tolist()
    
            fm.horizontalBaseline = baseline[0]
            fm.verticalBaseline = baseline[1]
            fm.totalBaseline = baseline[2]
            
        else:
            ret = False

        return ret
       
# if the extraction fails it return None    
    def extractInfo(self):
        
        try:
            
            fm = FrameMetadata()
            fm._dataset_type = self._frame.datasetType
            fm._sensingStart = self._frame.getSensingStart()
            fm._sensingStop = self._frame.getSensingStop()
            fm._spacecraftName = self._frame.getInstrument().getPlatform().getSpacecraftName()
            try: fm._spacecraftName =  fm._spacecraftName.decode('utf-8')
            except: pass
            fm._lookDirection = self._lookDirectionMap[self._frame.getInstrument().getPlatform().pointingDirection]
            fm._doppler = self._frame.doppler
            fm._prf = self._frame.PRF
            fm._startingRange = self._frame.startingRange
            uorb = self._frame.orbit._unpackOrbit()
            fm._orbit = uorb

            # try since sometimes is and empty string. if so set it to None
            try:
                fm._frameNumber = int(self._frame.getFrameNumber())
            except:
                fm._frameNumber = None
            try:
                fm._orbitNumber = int(self._frame.getOrbitNumber())
            except:
                fm._orbitNumber = None
            try:
                fm._beamID = self._frame.getInstrument().getBeamNumber()
                fm._beamID = fm._beamID.decode('utf-8')
            except:
                fm._beamID = None
            try:
                fm._trackNumber = int(self._frame.getTrackNumber())
            except:
                fm._trackNumber = None
                self.extractTrack(fm)
                
            self.extractOrbitRepeat(fm)
            self.extractPlatform(fm)
            

            if self.computeBaseline(fm):
                # double check if during the baseline computation somebody else became a master
                if(self.masterExists(fm) and fm.reference):
                    # if so recompute the baseline
                    self.computeBaseline(fm)
                self.computeLatitudeIndeces(fm)
                self.computeFrameID(fm)

        except Exception as e:
            print(e)
            raise Exception
        
        return fm
    
    def masterExists(self, fm):
        uu = UrlUtils()
        extremes = fm.getExtremes(fm.bbox)
        latMin = extremes[0]
        latMax = extremes[1]
        latDelta = (latMax - latMin) / 3.
        latitudeResolution = .1
        params = {
            'sensor': fm.platform,
            'trackNumber':fm.trackNumber,
            'latitudeIndexMin': int(math.floor((latMin - latDelta)/latitudeResolution)),
            'latitudeIndexMax': int(math.ceil((latMax + latDelta)/latitudeResolution)),
            'dataset_type':fm.dataset_type,
            'system_version':uu.version,
            'direction':fm.direction,
            'lookDirection':fm.lookDirection,
            'reference':True,
            }
        if fm.beamID:
            params['beamID'] =  fm.beamID
        exists = False
        metList,status = postQuery(buildQuery(params,['within']))
        if(status):
            metObj = createMetaObjects(metList)
            if(len(metObj) > 1):
                print("WARNING FrameInfoExtractor: Expecting only one frame to be reference")
            if metObj:
                exists = True
        return exists
    

def main(argv):
    import pdb
    pdb.set_trace()
    FI = FrameInfoExtractor()
    fm = FI.extractInfoFromFile(argv[0])
    print(fm.bbox) 

if __name__ == "__main__":
    import sys
    argv = sys.argv[1:]
    sys.exit(main(argv))
