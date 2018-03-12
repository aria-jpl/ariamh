#! /usr/bin/env python3: 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import sys

from isceobj.Util import key_of_same_content
from interferogram.createPrepareInterferogram import createPrepareInterferogram
from frameMetadata.FrameMetadata import FrameMetadata
from frameMetadata.FrameInfoExtractor import FrameInfoExtractor
from iscesys.Parsers.FileParserFactory import createFileParser
from utils.contextUtils import toContext
import urllib.request, urllib.error, urllib.parse
import json
import os
import math
import shutil
from datetime import datetime as dt
from utils.createImage import createImage
import pickle as pk
class Interferogram(object):


    def __init__(self):
        self._prepareInterferogram = None 
        # list of lists of the metadata (one for master and one for slave)
        self._master = "master.raw"
        self._slave = "slave.raw"
        self._pngSize = 512000
        self._geofile = ''
        # product name used for metadata final file
        self._amplitude = 'amplitude.geo'
        self._productName = 'interferogram'
        # place holder for the bbox of the geocoded image
        self._imageCorners = []
        # contains a possible expception
        self._except = ''
        self._sensor = ''
        #self._listPng = ['filt_corrections_topophase.flat.geo','filt_corrections_topophase.unw.geo','topophase.flat.geo','topophase.cor.geo']
        self._listPng = []
        #list of files to move
        #need to refactor a bit to set these filenames as property and not hardcoded
        self._geocodeList = []
        self._productList = []
        self._productListAux = []
        self._mdx = 'mdx.py'
        self._inputFile = "insar.xml"
        self._project = None
        self._insar = None
        self._insarClass = None
        self._extraGeoList = []
        self._insarPckName = 'insar.pck'

    

    def createInputFile(self,listMeta,inputs):
        inList,urls = self._prepareInterferogram.createInputList(listMeta,self._master,self._slave)
        import time
        dl_t0 = time.time()
        #for csk the list are tar files that need to be opened and extract the csk file name
        #so make the retrieveInputFile method return metadata file and construct the list
        inList = []
        j = 0
        for ms in urls:
            msList = []
            for i in range(len(ms[0])):
                msList.append(self._prepareInterferogram.retrieveInputFile(ms[0][i],ms[1][i]))
            inList.append([msList,'output_'+ str(j) + '.raw' ])
            j += 1
        outFile = self._prepareInterferogram.createInputFile(inList,inputs)
        dl_t1 = time.time()
        print(("Total download time: %s" % (dl_t1-dl_t0)))
        return outFile
    
    def createMetadata(self,filename):
        fp = open(filename)
        listMeta = json.load(fp)
        fp.close()
        ret = []
        for listFrame in listMeta:
            frames = []
            for frame in listFrame:
                fm = FrameMetadata()
                fm.load(frame)
                frames.append(fm)
            ret.append(frames)
        return ret


    def run(self,inputs):
        filename = inputs['createInterferogram']['inputFile']
        if 'productList' in inputs['createInterferogram']:
            self._productListAux = inputs['createInterferogram']['productList']
        self._productList.append(filename); 
        #try:
        
        process = 'Interferogram'
        try: 
            
            listMeta = self.createMetadata(filename)
            self._sensor = listMeta[0][0].spacecraftName
            #db start
            #self._sensor = 'CSKS4'
            #db end
            self._prepareInterferogram = createPrepareInterferogram(self._sensor)
            self._inputFile = self.createInputFile(listMeta,inputs)
            # hack to make isce believe this is the command line
            self._insar = self._insarClass(cmdline=self._inputFile)
            self._insar.configure()
            #these tow statements need to be here before configure in order to be set
            self._insar._insar.geocode_bbox = self.createGeoBBox(listMeta)
            #self._insar._insar.geocode_list = self.createGeocodeList(self._insar._insar)
            self._insar._configure()
            self._insar.run()
            #here dump insar object
            # delete it and reload from file
            pk.dump(self._insar._insar,open(self._insarPckName,'wb'))
            self.createPngList(self._insar._insar)
            self.createBrowseImages()
            self.createProductList()
            self.createProductJson(listMeta)
        except Exception as e:
            message = 'Interferogram.py: run failed with exception ' + str(e)
            exit = 1
            toContext(process,exit,message)
            raise
        exit = 0 
        message = 'Interferogram: completed'
        toContext(process,exit,message)
        return 0
    '''
    def createGeocodeList(self,insar):
        for togeo in self._extraGeoList:
            toAdd = getattr(insar,togeo)
            if(toAdd):
                self._geocodeList.append(toAdd)
        
        return self._geocodeList 
    '''
    def createPngList(self,insar):
        self._listPng.append(insar.coherenceFilename + '.geo')
        self._listPng.append(insar.unwrappedIntFilename + '.geo')
        
	####
    #### NEED to modify so that all the product list is user configurable
    ####

    def createProductList(self):
        listFiles = os.listdir('./')
        for fl in listFiles:
            if(fl.count('.geo')):#this add all the geocoded images + metadata + png
                self._productList.append(fl)
        self._productList.extend(['insarProc.xml','isce.log',self._insarPckName])
        for fl in self._productListAux:
            if not fl in self._productList and os.path.exists(fl):
                self._productList.append(fl)
                if(os.path.exists(fl+'.xml') and not (fl+'.xml' in self._productListAux)):
                    self._productList.append(fl+'.xml')
    def createGeoBBox(self,listMeta):
        #put values that will alway be ovewritten
        south = 100
        north = -100
        east = -200
        west = 200
        for meta in listMeta:
            for frame in meta:
                for bb in frame.refbbox:
                    if bb[0] < south:
                        south = bb[0]
                    if bb[0] > north:
                        north = bb[0]
                    if bb[1] > east:
                        east = bb[1]
                    if bb[1] < west:
                        west = bb[1]
        return [south,north,west,east]
    def createIntMeta(self,listMeta):
        fie = FrameInfoExtractor()
        # copy one of the frame since we can reuse some of the metadata
        fm = listMeta[0][0]
        fm.orbitNumber = [listMeta[0][0].orbitNumber,listMeta[1][0].orbitNumber]
        fm._spacecraftName = [listMeta[0][0].spacecraftName,listMeta[1][0].spacecraftName] 
        #sort frames by time
        toSort = []
        for meta in listMeta[0]:#take one set of frames 
            toSort.append([meta.sensingStart,meta])
        master = sorted(toSort)
        toSort = []
        for meta in listMeta[1]:#take other set of frames 
            toSort.append([meta.sensingStart,meta])
        slave = sorted(toSort)
        #the concept of reference becomes null when stitching and interfering frames
        fm.reference = False
        
        # this should be the same for all them otherwise some wrong happend
        fm.beamID = master[0][1].beamID

        # bbox
        #now is sorted by time, to take the early from the first and the late from the last
        early = master[0][1].bbox
        nearEarly = early[0]
        farEarly = early[1]
        late = master[-1][1].bbox
        nearLate = late[2]
        farLate = late[3]
        fm.bbox = [nearEarly,farEarly,nearLate,farLate]
        
        # do the same with the refernce 
        early = master[0][1].refbbox
        nearEarly = early[0]
        farEarly = early[1]
        late = master[-1][1].refbbox
        nearLate = late[2]
        farLate = late[3]
        fm.refbbox = [nearEarly,farEarly,nearLate,farLate]
        

        if((len(master)%2) == 0):#even, take the two in the middle
            midSceneMs1 = master[len(master)//2 - 1][1]
            midSceneMs2 = master[len(master)//2][1]
            midSceneSl1 = slave[len(slave)//2 - 1][1]
            midSceneSl2 = slave[len(slave)//2][1]
            #startRange from mid scene
            fm.startingRange = (midSceneMs1.startingRange + midSceneMs2.startingRange)/2.
            #baseline from mid scene
            fm.horizontalBaseline = [(midSceneMs1.horizontalBaseline + midSceneMs2.horizontalBaseline)/2.,(midSceneSl1.horizontalBaseline + midSceneSl2.horizontalBaseline)/2.]
            fm.verticalBaseline = [(midSceneMs1.verticalBaseline + midSceneMs2.verticalBaseline)/2.,(midSceneSl1.verticalBaseline + midSceneSl2.verticalBaseline)/2.]
            fm.totalBaseline = [(midSceneMs1.totalBaseline + midSceneMs2.totalBaseline)/2.,(midSceneSl1.totalBaseline + midSceneSl2.totalBaseline)/2.]
        else:#odd take the middle
            midSceneMs = master[len(master)//2][1]
            midSceneSl = slave[len(slave)//2][1]
            #startRange from mid scene
            fm.startingRange = midSceneMs.startingRange
            #baseline from mid scene
            fm.horizontalBaseline = [midSceneMs.horizontalBaseline,midSceneSl.horizontalBaseline]
            fm.verticalBaseline = [midSceneMs.verticalBaseline,midSceneSl.verticalBaseline]
            fm.totalBaseline = [midSceneMs.totalBaseline,midSceneSl.totalBaseline]
        
        #sensing start/stop from first and last frame
        fm.sensingStart = [master[0][1].sensingStart,slave[0][1].sensingStart]
        fm.sensingStop = [master[-1][1].sensingStop,slave[-1][1].sensingStop]
        sum  = 0
        for i in range(len(master)):
            sum +=  master[i][1].doppler
        sum1  = 0
        for i in range(len(slave)):
            sum1 +=  slave[i][1].doppler

        fm.doppler =  [sum/len(master),sum1/len(slave)]
        fm.url = ''
        minLat,maxLat,minLon,maxLon = self.getGeoLocation()
        fm.latitudeIndexMin = int(math.floor(minLat/fie._latitudeResolution))
        fm.latitudeIndexMax = int(math.ceil(maxLat/fie._latitudeResolution))

        return fm

    def getGeoLocation(self):
        parser = createFileParser('xml')
        #get the properties from the one of the geo files 
    
        prop, fac, misc = parser.parse('topophase.cor.geo.xml')
        coordinate1  =  key_of_same_content('coordinate1',prop)[1]
        width = float(key_of_same_content('size',coordinate1)[1])
        startLon = float(key_of_same_content('startingValue',coordinate1)[1])
        deltaLon = float(key_of_same_content('delta',coordinate1)[1])
        endLon = startLon + deltaLon*width
        coordinate2  =  key_of_same_content('coordinate2',prop)[1]
        length = float(key_of_same_content('size',coordinate2)[1])
        startLat = float(key_of_same_content('startingValue',coordinate2)[1])
        deltaLat = float(key_of_same_content('delta',coordinate2)[1])
        endLat = startLat + deltaLat*length
        minLat = min(startLat,endLat)
        maxLat = max(startLat,endLat)
        minLon = min(startLon,endLon)
        maxLon = max(startLon,endLon)
        return minLat,maxLat,minLon,maxLon

    def createProductJson(self,listMeta):
        fm = self.createIntMeta(listMeta)
        fie = FrameInfoExtractor()
        dic = fm.toDict() 
        minLat,maxLat,minLon,maxLon = self.getGeoLocation()
        latitudeIndexMin = int(math.floor(minLat/fie._latitudeResolution))
        latitudeIndexMax = int(math.ceil(maxLat/fie._latitudeResolution))
        #insar.main()
        jsonD = {"inputFile":self._inputFile,"product_type":self._productName,"imageCorners":{"minLat":minLat,"maxLat":maxLat,
                  "minLon":minLon,"maxLon":maxLon}}
        jsonD.update(dic)
        ss = jsonD["sensingStart"]
        s1 = ''.join(ss[0].split('T')[0].split('-'))
        s2 = ''.join(ss[1].split('T')[0].split('-'))
        prdName = jsonD['product_type'] + '_T' + str(jsonD['trackNumber']) + '_F' + str(latitudeIndexMin) + '-' + str(latitudeIndexMax)  + '_' + fm.spacecraftName[0] + '_' + s1 + '-' + fm.spacecraftName[1] + '_' +  s2 + '_' + dt.now().isoformat().replace(':','')
        dirName = prdName 
        try:
            os.mkdir(dirName)
        except:
            pass
        fp = open(os.path.join(dirName,prdName + '.met.json'),'w')
        json.dump(jsonD,fp,indent=4)
        fp.close()
        #just in case the default self._inputFile has been modified
        self._productList.append(self._inputFile)
        for fileName in self._productList:
            shutil.move(fileName,dirName)

    def createBrowseImages(self):
        import math
        for name in self._listPng:
            '''
            if(name.count(self._corrections)):
                command = 'mdx.py -P ' + name + ' -cmap cmy -wrap 20'
                self.saveImage(command,name + '_20rad')
            '''
            if name.count('unw.geo'):
                command = 'mdx.py -P ' + name
                createImage(command,name)
                command = 'mdx.py -P ' + name + ' -wrap 20'
                createImage(command,name + '_20rad')
                parser = createFileParser('xml')
                #get the properties from the one of the geo files 
                prop, fac, misc = parser.parse(name + '.xml')
                coordinate1  =  key_of_same_content('coordinate1',prop)[1]
                width = int(key_of_same_content('size',coordinate1)[1])
                command = 'mdx -P ' + name + ' -s ' + str(width) + ' -amp -r4 -rtlr ' + str(int(width)*4) + ' -CW'
                createImage(command,self._amplitude)
            elif name.count('cor.geo'):
                command = 'mdx.py -P ' + name
                createImage(command,name)
                parser = createFileParser('xml')
                #get the properties from the one of the geo files 
                prop, fac, misc = parser.parse(name + '.xml')
                coordinate1  =  key_of_same_content('coordinate1',prop)[1]
                width = int(key_of_same_content('size',coordinate1)[1])
                command = 'mdx -P ' + name + ' -s ' + str(width) + ' -r4 -rhdr ' + str(int(width)*4) + ' -cmap cmy -wrap 1.2'
                createImage(command,name.replace('.cor.geo','_ph_only.cor.geo'))
