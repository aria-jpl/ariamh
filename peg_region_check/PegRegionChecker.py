 #! /usr/bin/env python3
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2011, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


import sys

import pdb
import os
import math
import json
from httplib2 import Http
from urllib.parse import urlencode
from utils.UrlUtils import UrlUtils
from datetime import datetime as dt, timedelta as td
from frameMetadata.FrameInfoExtractor import FrameInfoExtractor
from frameMetadata.FrameMetadata import FrameMetadata
from peg_region_check.PegReader import PegReader, PegInfoFactory
from iscesys.Compatibility import Compatibility
from utils.queryBuilder import postQuery,buildQuery,createMetaObjects
Compatibility.checkPythonVersion()

fmt = '%Y-%m-%dT%H:%M:%S.%f'


class PegRegionChecker:
    globalMock = 0
    STATUS_QUERY_OK = '200'
    def initPegList(self,filename = None):
        if (filename == None):
            if(self._pegFilename == None):
                print('Warning: Cannot initialize peg list. Filename is needed.')
                return
        else:    
            self._pegFilename = filename
        PR = PegReader()
        self._pegList = PR.createPegList(self._pegFilename)

    def getPegFile(self,sensor,project):
        #factory for the pegfile based on sensor and project.
        #the convention for the filename is prgfile_sensorLowercase_projectLowecase 
      
        if(project.lower().count('trigger')):
            filename = os.path.join('pegfile_' + sensor.lower() + '_' + project.lower())
        else:
            filename = os.path.join(os.environ['ARIAMH_HOME'],'conf', 'pegfile_' + sensor.lower() + '_' + project.lower())
        if not (os.path.exists(filename)):
            print("Cannot find peg file ", filename)
            raise Exception
        return filename
    
    def findPegRegion(self,bbox,track):
        maxLat = -1000
        minLat = 1000
        maxLon = -1000
        minLon = 1000
        for bb in bbox:
            if(bb[0] > maxLat):
                maxLat = bb[0]
            if(bb[0] < minLat):
                minLat = bb[0]
            if(bb[1] > maxLon):
                maxLon = bb[1]
            if(bb[1] < minLon):
                minLon = bb[1]
        pegIndx = [] # a frame can cross 2 peg regions
        #print (maxLat,minLat)
        for i in range(len(self._pegList)):
            #import pdb
            #pdb.set_trace()            
            if (track == self._pegList[i].track) :
                pegLon = self._pegList[i].peg.getLongitude()
                #this is  a way to make sure that we are looking at the right track, since for each track # there is a descending and an ascending
                if (math.fabs(pegLon - (maxLon + minLon)/2.0) < 90) or (math.fabs(pegLon - (maxLon + minLon)/2.0) > 270):
                    maxPegLat = self._pegList[i].latStart
                    minPegLat = self._pegList[i].latEnd
                    #print (minPegLat,maxPegLat)
                    if(self._pegList[i].latStart < self._pegList[i].latEnd):
                        minPegLat = self._pegList[i].latStart
                        maxPegLat = self._pegList[i].latEnd
                    if (maxLat < maxPegLat) and (minLat > minPegLat):# is fully contained
                        pegIndx.append(i)
                        break
                    elif (maxLat < minPegLat) or (minLat > maxPegLat):# fully out go to next
                        continue
                    else:# the frame crosses the peg extremes. append but do not break since probably it will cross another region
                        pegIndx.append(i)


        if len(pegIndx) == 0:
            print("Warning: Cannot find a matching peg regions for given frame." )
        return pegIndx

    def checkPegRegionCoverage(self,peg,bboxes):
        #check the extremes because there might be two that are over the latbands. take only the closest
        pegStart = min(peg.latStart,peg.latEnd)
        pegEnd = max(peg.latStart,peg.latEnd)
        frameLen = math.fabs(bboxes[0][0][0] - bboxes[0][2][0]) # diff of latitudes from ealy and late acquisition for first frame
        pegLen = pegEnd - pegStart
        pegLon = peg.peg.getLongitude()
        #the orbit might be ascending or descending. to discriminate check the vicinity of the peg lon and the frame lon
        numDiv = int(math.fabs((max(pegLen/frameLen,1))*10))
        delta = pegLen/numDiv
        start = pegStart
        pointList = [start + i*delta for i in range(numDiv+1)] # this should have enough sampling of the region including the edges of the peg region
        # now check that all the point are covered
        pointIn = [0]*(numDiv+1)
        for i in range(len(pointList)):
            for j in range(len(bboxes)):
                maxLon = -1000
                minLon = 1000
                for bb in bboxes[j]:
                    if(bb[1] > maxLon):
                        maxLon = bb[1]
                    if(bb[1] < minLon):
                        minLon = bb[1]
                #this is  a way to make sure that we are looking at the right track, since for each track # there is a descending and an ascending
                if not (math.fabs(pegLon - (maxLon + minLon)/2.0) < 90) or (math.fabs(pegLon - (maxLon + minLon)/2.0) > 270):
                    continue
                maxLat = -1000
                minLat = 1000
                for bb in bboxes[j]:
                    if(bb[0] > maxLat):
                        maxLat = bb[0]
                    if(bb[0] < minLat):
                        minLat = bb[0]

                if(pointList[i] <= maxLat and pointList[i] >= minLat):
                    pointIn[i] = 1

        if(sum(pointIn) != len(pointIn)):
            retVal = []
        else:
            bboxes = sorted(bboxes,reverse = True) #it will sort by the first lat of each bbox.
                                                   #descending order w.r.t. the first element of the bb
            for i in range(len(bboxes)):
                minLat = 1000
                for b in bboxes[i]:
                    minLat = min(b[0],minLat)
                if minLat <= pegStart:#this is the first frame needed
                    bboxes = bboxes[:i+1]
                    break
            #do the same for the top of the peg band
            bboxes = sorted(bboxes)

            for i in range(len(bboxes)):
                maxLat = -1000
                for b in bboxes[i]:
                    maxLat = max(b[0],maxLat)
                if maxLat >= pegEnd:#this is the first frame needed
                    bboxes = bboxes[:i+1]
                    break

            retVal = bboxes


        return retVal
    

    def refineMetadataList(self,metList,bboxes):
                
        #go through each of the metalist and keep only the one whose bounding boxes are present in bboxes.
        #remove the other since contain unnecessary frames
        retMet = []
        for frame in metList:
            bbNow = frame.refbbox[0]
            for bb in bboxes:
                if(math.fabs(bb[0][0] - bbNow[0]) < .0001):#they are the same, keep it
                    retMet.append(frame)
                    break

        return retMet
    def mockRestCall(self):
        #remember to remove globalMock from the top 
        if(PegRegionChecker.globalMock == 0):
            extractL = ['IMG-HH-ALOS2050286350-150429-FBDR1.1__A.json']
            PegRegionChecker.globalMock = 1
        else:
            extractL = ['IMG-HH-ALOS2042006350-150304-FBDR1.1__A.json']
        retL = []
        for name in extractL:
            '''
            fm = FrameMetadata()
            fm.load(name)
            '''
            fm = json.load(open(name))
            retL.append(fm)
        return retL,True
    
    #returns a list of frames that are included in the peg region
    def searchMasterFrames(self,peg):
        uu = UrlUtils()
        beamID = self._frame.beamID
        direction = self._frame.direction
        lat_min = min(peg.latStart,peg.latEnd)
        lat_max = max(peg.latStart,peg.latEnd)
        params = {
            'platform'  : self._frame.spacecraftName,
            'trackNumber':self._frame.trackNumber,
            'dataset_type':self._frame.dataset_type,
            'beamID':beamID,
            'system_version':uu.version,
            'latitudeIndexMin': int(math.floor((lat_min)/FrameInfoExtractor._latitudeResolution)),
            'latitudeIndexMax': int(math.floor((lat_max)/FrameInfoExtractor._latitudeResolution)),
            'direction': direction
            }

        #get the list of meta close to the reference frame
        metList = postQuery(buildQuery(params,['cross-boundaries']))

        if metList[0]:
            metList = self.refineFromTime(metList[0],self._frame.sensingStart,self._maxTimeStitch)
        return metList
    
    #returns a list of lists of lists of frames that covers a given peg region grouped by 
    #platforms and acquisition time
    def searchSlaveFrames(self,peg):
        uu = UrlUtils()
        beamID = self._frame.beamID
        direction = self._frame.direction
        lat_min = min(peg.latStart,peg.latEnd)
        lat_max = max(peg.latStart,peg.latEnd)
        params = {
            'sensor'  : self._frame.platform,
            'trackNumber':self._frame.trackNumber,
            'dataset_type':self._frame.dataset_type,
            'beamID':beamID,
            'system_version':uu.version,
            'latitudeIndexMin': int(math.floor((lat_min)/FrameInfoExtractor._latitudeResolution)),
            'latitudeIndexMax': int(math.floor((lat_max)/FrameInfoExtractor._latitudeResolution)),
            'direction': direction
            }

        #get the list of meta close to the reference frame 
        metList = postQuery(buildQuery(params,['cross-boundaries']))
        
        if metList:
            metList = self.refineByPlatform(metList[0])
        newMet = []
        for met in metList:
            newMet.append(self.groupByTime(met))
        return newMet
    
    def groupByTime(self,metList):
        
        metDict = {}       
        while True:
            metNow = metList[0]
            key = metNow['sensingStart']
            metDict[key] = []
            date = dt.strptime(key,fmt)
            numEl = len(metList)
            #start from last so one can just pop the elements
            for i in range(numEl-1,-1,-1):
                met = metList[i]
                date1 = date1 = dt.strptime(met['sensingStart'],fmt)
                if date1 > date - self._maxTimeStitch and date1 < date + self._maxTimeStitch:
                    metDict[key].append(met)
                    metList.pop(i)
            if len(metList) == 0:
                break
        ret = []
        for v in metDict.values():
            ret.append(v)
        return ret
         
    #queryKey can be sensor or platform for the query, but for the FrameMetadata they get mapped
    #into sensor -> platform, platform -> spacecraftName  
    def metaQuery(self,queryKey,peg,date,delta):
        
        uu = UrlUtils()
        beamID = self._frame.beamID
        direction = self._frame.direction
        lat_min = min(peg.latStart,peg.latEnd)
        lat_max = max(peg.latStart,peg.latEnd)
        params = {
            queryKey  : getattr(self._frame,self._frame._mappingLoad[queryKey]),
            #'orbitNumber':orbit,
            'beamID':beamID,
            'system_version':uu.version,
            'latitudeIndexMin': int(math.floor((lat_min)/FrameInfoExtractor._latitudeResolution)),
            'latitudeIndexMax': int(math.floor((lat_max)/FrameInfoExtractor._latitudeResolution)),
            'direction': direction
            }

        metList = postQuery(buildQuery(params,['cross-boundaries']))

        if metList:
            metList = [self.refineFromTime(metList[0],date,delta),metList[1]]
            metList = [self.refineByPlatform(metList[0]),metList[1]]
        return metList

    #only keep the frames between date +- delta
    def refineFromTime(self,metList,date,delta):
        ret = []

        for met in metList:
            date1 = dt.strptime(met['sensingStart'],fmt)
            if date1 > date - delta and date1 < date + delta:
                ret.append(met)
        return ret
    #since there might be multiple data in the same location from different satellites
    #belonging to a constellation, separate them
    def refineByPlatform(self,metList):
        platforms = []
        for met in metList:
            if not met['platform'] in platforms:
                platforms.append(met['platform'])
        newList = []
        for pl in platforms:
            ls = []
            for met in metList:
                if pl == met['platform']:
                    ls.append(met)
            newList.append(ls)
     
        return newList       
            
    def createMeta(self,metaDict):
        fm = FrameMetadata()
        fm.load(metaDict)
        return fm

    def isCovered(self,peg,metList):
        retList = []        
        bboxes = []
        for fm in metList:
            bboxes.append(fm.refbbox)
        
        ret = self.checkPegRegionCoverage(peg,bboxes)
        if(ret):
            retList = self.refineMetadataList(metList,ret)
        return retList

 

    def getPegToUse(self,maxLat,minLat,lon,track):
        pegList = []
                
        for i in range(len(self._pegList)): 
            if track == self._pegList[i].track:
                maxPegLat = max(self._pegList[i].latStart,self._pegList[i].latEnd)
                minPegLat = min(self._pegList[i].latStart,self._pegList[i].latEnd)
                pegLon = self._pegList[i].peg.getLongitude()
                if (math.fabs(pegLon - lon) > 90) and (math.fabs(pegLon - lon) < 270):
                    continue
                #see if the center of the frames falls in this peg region 
                if ((maxLat + minLat)/2.0 <= maxPegLat and (maxLat + minLat)/2.0 >= minPegLat):
                    pegList.append(self._pegList[i])
                    # to have a better estimate of the heading save the prev and next region if adjacent
                    if i + 1 < len(self._pegList):
                        if (self._pegList[i+1].track == self._pegList[i].track):
                            pegList.append(self._pegList[i+1])
                    if i - 1 >= 0:
                        if (self._pegList[i-1].track == self._pegList[i].track):
                            pegList.append(self._pegList[i-1])
                    break
        return pegList
    def estimatePeg(self,dictFrames):
        track = int(dictFrames[0]['TrackNumber'][0])
        for dictNow in dictFrames:
            if not int(dictNow['TrackNumber'][0]) == track:
                print('Error: Frames do not belong to the same track')
                raise Exception
        bboxes = self.extractBboxesFromDict(dictFrames)
        maxLat = -1000
        minLat = 1000
        for j in range(len(bboxes)):
            for bb in bboxes[j]:
                if(bb[0] > maxLat):
                    maxLat = bb[0]
                if(bb[0] < minLat):
                    minLat = bb[0]
        
        maxLon = -1000
        minLon = 1000
        for j in range(len(bboxes)):
            for bb in bboxes[j]:
                if(bb[1] > maxLon):
                    maxLon = bb[1]
                if(bb[1] < minLon):
                    minLon = bb[1]
         
        pegList = self.getPegToUse(maxLat,minLat,(minLon + maxLon)/2.0,track)
        if pegList == []:
            print('Error: Cannot find a matching  Peg region for the frames provided.')
            raise Exception
        pegLat = (minLat+ maxLat)/2.0
        if len(pegList) == 1:
            pegLon = pegList[0].peg.longitude
            pegLat = pegList[0].peg.latitude
            hdg = pegList[0].peg.heading

        elif len(pegList) == 2:#do linear fit y = a*x+b of the heading.  it returns [a,b]
            #assume the center of the scene as the point associated to the heading
            x0 = (pegList[0].latStart + pegList[0].latEnd)/2.0
            y0 = pegList[0].peg.heading
            x1 = (pegList[1].latStart + pegList[1].latEnd)/2.0
            y1 = pegList[1].peg.heading
            ab = self.findLine([[x0,x1],[y0,y1]])
            hdg = ab[0]*pegLat + ab[1]
            #do the same thing for the longitude
            y0 = pegList[0].peg.longitude
            y1 = pegList[1].peg.longitude
            ab = self.findLine([[x0,x1],[y0,y1]])
            pegLon = ab[0]*pegLat + ab[1]

        elif len(pegList) == 3:#find the parabula passing through the three points
            #assume the center of the scene as the point associated to the heading
            x0 = (pegList[0].latStart + pegList[0].latEnd)/2.0
            y0 = pegList[0].peg.heading
            x1 = (pegList[1].latStart + pegList[1].latEnd)/2.0
            y1 = pegList[1].peg.heading
            x2 = (pegList[2].latStart + pegList[2].latEnd)/2.0
            y2 = pegList[2].peg.heading
            abc = self.findParabula([[x0,x1,x2],[y0,y1,y2]])
            hdg = abc[0]*pegLat**2 + abc[1]*pegLat + abc[2]
            #do the same thing for the longitude
            y0 = pegList[0].peg.longitude
            y1 = pegList[1].peg.longitude
            y2 = pegList[2].peg.longitude
            abc = self.findParabula([[x0,x1,x2],[y0,y1,y2]])
            pegLon = abc[0]*pegLat**2 + abc[1]*pegLat + abc[2]
        dire = pegList[0].direction
        band = 0 #not used
        if dire == 'asc':
            latE = maxLat
            latS = minLat
        elif dire == 'dsc':
            latS = maxLat
            latE = minLat
        else:
            print('Error: Wrong peg direction:',dire)
            raise Exception
        pegToProcess = PegInfoFactory.createPegInfo(band,track,dire,latS,latE,pegLat,pegLon,hdg)
        return pegToProcess

    #determinant for a 3x3 matrix
    def computeDet(self,mat):
        return mat[0][0]*(mat[1][1]*mat[2][2] - mat[1][2]*mat[2][1]) -  mat[0][1]*(mat[1][0]*mat[2][2] - mat[1][2]*mat[2][0]) + mat[0][2]*(mat[1][0]*mat[2][1] - mat[1][1]*mat[2][0])
        
    def findParabula(self,xy): #xy = 3 pairs of (xi,yi) . find solution like ax^2+bx+c = d. solve 3 x 3 eq using Kramer rule
        
        #xy[0] are the xs and xy[1] are the ys 
        retVal = []
        c = [1,1,1] # the coeff of c are all 1
        a = [xy[0][0]**2,xy[0][1]**2,xy[0][2]**2] # these are the x^2
        b = [xy[0][0],xy[0][1],xy[0][2]]#these are the x
        d = xy[1]
        mat = [a,b,c]
        mata = [d,b,c]
        matb = [a,d,c]
        matc = [a,b,d]
        det = self.computeDet(mat)
        if (math.fabs(det) < 10**-20): # crappy solution
            retVal = []
        else:

            deta = self.computeDet(mata)
            detb = self.computeDet(matb)
            detc = self.computeDet(matc)
            retVal = [deta/det,detb/det,detc/det]
        
        return retVal 
       
    def findLine(self,xy):#
        a = (xy[1][1] - xy[1][0])/(xy[0][1] - xy[0][0])
        b = xy[1][0] - a*xy[0][0]
        return [a,b]
    def coverSameRegion(self,l1,l2):
        min1 = 1000
        max1 = -1000
        min2 = 1000
        max2 = -1000
        bb = l1[0].refbbox
        frameSize = math.fabs(bb[0][0] - bb[2][0])
        delta = frameSize/5
        for el in l1:
            bb = el.refbbox[0][0] #just taking the first lat of the bbox
            if(bb < min1):
                min1 = bb
            if(bb > max1):
                max1 = bb

        for el in l2:
            bb = el.refbbox[0][0] #just taking the first lat of the bbox
            if(bb < min2):
                min2 = bb
            if(bb > max2):
                max2 = bb
        coverSame = False
        if(math.fabs(max1-max2) < delta  and math.fabs(min1 - min2) < delta):#the two tops and two bottoms are close enough
            coverSame = True
        return coverSame
         


    def runNominalMode(self):
        FM = self._frame
        
        #use sensing start as uniqueness for a give frame
        self._referenceFrame = FM.sensingStart
        track = FM.trackNumber
        #orbitRepeat = int(met['OrbitRepeat'][0])
        bbox = FM.refbbox
        pegIndx = self.findPegRegion(bbox,track)
        
        toBeProcessed = []
        associatedPegs = []
        for indx in pegIndx:
            peg = self._pegList[indx]
            masterFrames = self.searchMasterFrames(peg)
            
            
            if masterFrames:
                master = self.isCovered(peg,createMetaObjects(masterFrames))
                if master:  
                    slaveFrames  = self.searchSlaveFrames(peg)
                    slaves = []
                    
                    for framep in slaveFrames:#organized by platform
                        for framet in framep:#organized by time
                            meta = createMetaObjects(framet)
                            #remove the master from the list
                            isMaster = False
                            for el in meta:
                                if el.sensingStart == self._referenceFrame:
                                    isMaster = True
                                    break
                            if isMaster:
                                continue
                            ret = self.isCovered(peg,meta)
                            if ret:
                                slaves.append(ret)
                            
                    
           
                    isIn = False
                    #at least one should contain the initial frame
                    for el in master:
                        if el.sensingStart == self._referenceFrame:
                            isIn = True
                            break
                    '''
                    if isIn:#print the bbox ans lat bands to see f they are consistent
                        print("PegRegionChecker: lat bands",northLat,southLat)
                        for el in master:
                            print(el.refbbox)
                    '''
                    if not isIn:
                        continue
                    for slave in slaves:
                        if(not self.coverSameRegion(slave,master)):
                            continue
                        if(len(slave) != len(master)):
                            slave = self.cleanOrbitList(slave)
                            master = self.cleanOrbitList(master)


                        toBeProcessed.append([slave,master])
                        associatedPegs.append(peg)
                        
                        if(self._breakAfterFirst):
                            break
        return toBeProcessed, associatedPegs

    def cleanOrbitList(self,orbit):
        newOrbit = []
        for i in range(len(orbit)):
            isIn = False
            for j in range(len(newOrbit)):
                if (orbit[i].sensingStart == newOrbit[j].sensingStart):
                    isIn = True
                    break
            if not isIn:
                newOrbit.append(orbit[i])
        return newOrbit
    
    def createDatesToSearch(self,date):
        self._datesToSearch = [date - i*self._deltaSearch - self._maxTimeStitch for i in range(self._searchUpTo)]
    
    def __getstate__(self):
        d = dict(self.__dict__)
        return d
    def __setstate__(self,d):
        self.__dict__.update(d)
        #self.logger = logging.getLogger("PegRegionChecker")

    def setSearchDirection(self,odir):
        self._searchDirection = odir
    def setPegFilename(self,name):
        self._pegFilename = name

    def __init__(self,frame = None, project = None):
        self._maxTimeStitch = td(seconds=60)
        uu = UrlUtils()
        self._frame = frame
        self.rest_url = uu.rest_url
        self.requester = Http()
        self._referenceFrame = ""
        self._pegList = []
        self._pegFilename = ""
        self._breakAfterFirst = False #when searching for multiple passes stop as soon as on orbit
                                    #covers the peg region. useful for trigger mode
        self._sensorType = None
        self._searchUpTo = 200
        self._deltaSearch = td(days=1)
        self._searchDirection = 0 #0 searches  (+-)self._searchOrbitUpTo from input frame orbit, 1  +self._searchOrbitUpTo and -1 -self._searchOrbitUpTo
        self._datesToSearch = []
       
        #single frame needs to be treated differently. It's hard to create an exact
        #peg region containing exactly one frame unless we know already all the refbbox.
        #in this case just create a peg region based on the refbbox of the input frame
        #so it's always satisfied
        if project.endswith('sf'):
            extremes = frame.getExtremes(frame.refbbox)
            delta = (extremes[1] - extremes[0])/5.
            if frame.direction == 'asc':
                latS = extremes[0] + delta
                latE = extremes[1] - delta
            else:
                latS = extremes[1] - delta
                latE = extremes[0] + delta
            self._pegList.append(PegInfoFactory.createPegInfo(0,frame.trackNumber
                                         ,frame.direction,latS,latE,(latS+latE)/2.,
                                         (extremes[2]+extremes[3])/2.,0))
        else:
            # if filename is provided in constructor, then initialize the pegList as well
            if frame.spacecraftName is not None and project is not None:
                self._pegFilename = self.getPegFile(frame.spacecraftName,project)
                self.initPegList()
                self._sensorType = frame.spacecraftName
                self._project = project
                

def main():
    import pdb
    pdb.set_trace()
    PR = PegRegionChecker()
    PR.initPegList('pegfile_alos.txt_test')
    PR._sensorType = "ALOS"
    #list1 = PR.mockRestCall()
    #list2 = PR.mockRestCall()
    FM = FrameMetadata()
    FM.load(sys.argv[1])
    tbp,peg = PR.runNominalMode(FM)
    print('dummy')
    '''
    lat0 = 33.2
    lon0 = -121.5
    delta = 0.1
    bboxes = []
    for i in xrange(4):
        lon0 +=i*delta
        bboxes.append([[lat0,lon0],[lat0+delta,lon0+1],[lat0-1,lon0 + delta],[lat0+delta-1,lon0 + 1 + delta]])
        lat0 -= 0.5
    pass
    track = 211
    pegIndx = PR.findPegRegion(bboxes[3],track)
    peg = PR._pegList[pegIndx[0]]
    res = PR.checkPegRegionCoverage(peg,bboxes)
    #PR.estimatePeg(bboxes)
    '''
if __name__ == "__main__":
   sys.exit(main())

