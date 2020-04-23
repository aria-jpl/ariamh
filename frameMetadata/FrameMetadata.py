#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2011, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


import os
import sys 
class FrameMetadata(object):
    
    def getSpacecraftName(self):
        return self._spacecraftName
    def getFrameName(self):
        return self._frameName
    def getUrl(self):
        return self._url
    def getOrbitNumber(self):
        return self._orbitNumber
    def getOrbit(self):
        return self._orbit
    def getOrbitRepeat(self):
        return self._orbitRepeat
    def getTrackNumber(self):
        return self._trackNumber
    def getFrameNumber(self):
        return self._frameNumber
    def getBBox(self):
        return self._bbox
    def getReferenceBBox(self):
        return self._refbbox
    def getSensingStart(self):
        return self._sensingStart
    def getSensingStop(self):
        return self._sensingStop
    def getDirection(self):
        return self._direction
    def getLookDirection(self):
        return self._lookDirection
    def getTotalBaseline(self):
        return self._totalBaseline
    def getHorizontalBaseline(self):
        return self._horizontalBaseline
    def getVerticalBaseline(self):
        return self._verticalBaseline
    def getDoppler(self):
        return self._doppler
    def getPRF(self):
        return self._prf
    def getReference(self):
        return self._reference
    def getStartingRange(self):
        return self._startingRange
    def getLatitudeIndexMin(self):
        return self._latitudeIndexMin
    def getLatitudeIndexMax(self):
        return self._latitudeIndexMax
    def getBeamID(self):
        return self._beamID
    def getFrameID(self):
        return self._frameID
    def getPlatform(self):
        return self._platform
    def getDatasetType(self):
        return self._dataset_type
    
    
    def setFrameName(self,var):
        self._frameName = var
    def setUrl(self,var):
        self._url = var
    
    def setOrbitNumber(self,val):
        self._orbitNumber = val
    def setOrbit(self,val):
        self._orbit = val
    def setOrbitRepeat(self,val):
        self._orbitRepeat = val
    def setTrackNumber(self,val):
        self._trackNumber = val
    def setFrameNumber(self,val):
        self._frameNumber = val
    def setSpacecraftName(self,val):
        self._spacecraftName = val
    def setBBox(self,val):
        self._bbox = val
    def setReferenceBBox(self,val):
        self._refbbox = val
    def setSensingStart(self,val):
        self._sensingStart = val
    def setSensingStop(self,val):
        self._sensingStop = val
    def setDirection(self,val):
        self._direction = val
    def setLookDirection(self,val):
        self._lookDirection = val
    def setTotalBaseline(self,val):
        self._totalBaseline = val
    def setHorizontalBaseline(self,val):
        self._horizontalBaseline = val
    def setVerticalBaseline(self,val):
        self._verticalBaseline = val
    def setDoppler(self,val):
        self._doppler = val
    def setPRF(self,val):
        self._prf = val
    def setReference(self,val):
        self._reference = val
    def setStartingRange(self,val):
        self._startingRange = val
    def setLatitudeIndexMin(self,val):
        self._latitudeIndexMin = val
    def setLatitudeIndexMax(self,val):
        self._latitudeIndexMax = val
    def setBeamID(self,val):
        self._beamID = val
    def setFrameID(self,val):
        self._frameID = val
    def setPlatform(self,val):
        self._platform = val
    def setDatasetType(self,val):
        self._dataset_type = val
    def toDict(self):
        jDict = {}

        #NOTE that we remove the _ from the attribute
        for attrk,attrv in list(self.__dict__.items()):
            if(attrk.find('mapping') >= 0):#NOTE that _has been removed
                continue
            if(attrk.find('_sensing') >= 0):#for datetime convert into a string
                if isinstance(attrv,list):
                    ret = []
                    for v in attrv:
                        ret.append(v.isoformat())
                    jDict[attrk[1:]] = ret
                else:
                    dt = attrv.isoformat()        
                    jDict[attrk[1:]] = dt 
            elif(attrk == '_orbit'):
                jDict[attrk[1:]] = [attrv[0],attrv[1],attrv[2],attrv[3].isoformat()]
            elif(attrk in self._mappingDump):
                jDict[self._mappingDump[attrk]] = attrv
            else:
                jDict[attrk[1:]] = attrv
        return jDict

    def getExtremes(self,bbox):
        minLat = 1000
        maxLat = -1000
        minLon = 1000
        maxLon = -1000
        for bb in bbox:
            if (bb[0] < minLat):
                minLat = bb[0]
            if (bb[1] < minLon):
                minLon = bb[1]
            if (bb[0] > maxLat):
                maxLat = bb[0]
            if (bb[1] > maxLon):
                maxLon = bb[1]
                
        return minLat,maxLat,minLon,maxLon
    def isInBbox(self,bboxRef):
        minLat,maxLat,minLon,maxLon = self.getExtremes(self.bbox)
        minLatR,maxLatR,minLonR,maxLonR = self.getExtremes(bboxRef)
        padLat = .1*(maxLatR - minLatR)
        padLon = .1*(maxLonR - minLonR)
        isIn = False
        if(minLat > minLatR - padLat and minLon > minLonR - padLon
           and maxLat < maxLatR + padLat and maxLon < maxLonR + padLon):
            isIn = True
        return isIn
        

    def dump(self,filename):
        import json
        fp = open(filename,'w')
        jDict = self.toDict()
        json.dump(jDict,fp,indent=4,sort_keys=True)
        fp.close()
    def load(self,input):
        import json
        from datetime import datetime
        #if filename is not a str see if it's already dictionary
        if(isinstance(input,str)):
            fp = open(input)
            jDict = json.load(fp)
            fp.close()
        elif(isinstance(input,dict)):
            jDict = input 
        else:
            print('Unrecognized input type')
            raise ValueError
        for k,attrv in list(jDict.items()):
            if k in self._mappingLoad:
                attrk = self._mappingLoad[k]
            else:
                attrk = k
            if not ('_' + attrk in self.__dict__):
                continue
            if(attrk.find('sensing') >= 0):#NOTE that _has been removed
                #this format has msec as well
                fmt = '%Y-%m-%dT%H:%M:%S.%f' # iso format
                if isinstance(attrv,list):
                    ret = []
                    for v in attrv:
                        ret.append(datetime.strptime(v,fmt))
                    setattr(self,attrk,ret)
                else:
                    dt =  datetime.strptime(attrv,fmt)
                    setattr(self,attrk,dt)
            
            elif(attrk == 'orbit'):
                # no msec in this format
                fmt = '%Y-%m-%dT%H:%M:%S' # iso format
                if(isinstance(attrv[3],str)):
                    try:
                        attrv[3] =  datetime.strptime(attrv[3],fmt)
                    except Exception:#see if the orbit has msec
                        fmt = '%Y-%m-%dT%H:%M:%S.%f'
                        attrv[3] =  datetime.strptime(attrv[3],fmt)
                setattr(self,attrk,attrv)
            else:
                setattr(self,attrk,attrv)

    def __init__(self):
        self._mappingLoad = {'platform':'spacecraftName','sensor':'platform'}
        self._mappingDump = {'_spacecraftName':'platform','_platform':'sensor'}

        self._spacecraftName = ''
        self._frameName = ''
        self._url = []
        self._orbitNumber = None
        self._orbitRepeat = None
        self._trackNumber = None
        self._frameNumber = None
        self._bbox = [] # [near start, far start, near end, far end]  
        self._refbbox = [] # [near start, far start, near end, far end]  of teh reference frame for that location
        self._sensingStart = None
        self._sensingStop = None
        self._direction = None
        self._lookDirection = None
        self._totalBaseline = None
        self._horizontalBaseline = None
        self._verticalBaseline = None
        self._doppler = None
        self._orbit = None
        self._prf = None
        self._reference = None
        self._startingRange = None
        self._latitudeIndexMax = None
        self._latitudeIndexMin = None
        self._platform = None
        self._beamID = None
        self._frameID = None
        self._dataset_type = None


    frameName = property(getFrameName,setFrameName)
    url = property(getUrl,setUrl)
    spacecraftName = property(getSpacecraftName,setSpacecraftName)
    orbitNumber = property(getOrbitNumber,setOrbitNumber)
    orbitRepeat = property(getOrbitRepeat,setOrbitRepeat)
    trackNumber = property(getTrackNumber,setTrackNumber)
    frameNumber = property(getFrameNumber,setFrameNumber)
    bbox =  property(getBBox,setBBox)
    refbbox =  property(getReferenceBBox,setReferenceBBox)
    sensingStart = property(getSensingStart,setSensingStart)
    sensingStop = property(getSensingStop,setSensingStop)
    lookDirection = property(getLookDirection,setLookDirection)
    direction = property(getDirection,setDirection)
    totalBaseline = property(getTotalBaseline,setTotalBaseline)
    horizontalBaseline = property(getHorizontalBaseline,setHorizontalBaseline)
    verticalBaseline = property(getVerticalBaseline,setVerticalBaseline)
    orbit = property(getOrbit,setOrbit)
    doppler = property(getDoppler,setDoppler)
    prf = property(getPRF,setPRF)
    reference = property(getReference,setReference)
    startingRange = property(getStartingRange,setStartingRange)
    latitudeIndexMax = property(getLatitudeIndexMax,setLatitudeIndexMax)
    latitudeIndexMin = property(getLatitudeIndexMin,setLatitudeIndexMin)
    beamID = property(getBeamID,setBeamID)
    frameID = property(getFrameID,setFrameID)
    platform = property(getPlatform,setPlatform)
    dataset_type = property(getDatasetType,setDatasetType)


def main(argv):
    import json
    fm = FrameMetadata()
    fm.load(argv[0])
    print(fm.isInBbox(fm.refbbox))
    
    pass

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
