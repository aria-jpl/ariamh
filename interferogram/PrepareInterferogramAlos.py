#! /usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import sys

from interferogram.InputFileCreatorAlos import InputFileCreatorAlos as IFC 
import urllib.request, urllib.error, urllib.parse
import json
import os
class PrepareInterferogramAlos(object):


    def __init__(self):
        pass
    
    def createInputList(self,master,slave):
        urlsList = []
        imgList = []
        ledList = []
        for frames in self._metaList:
            imgL = []
            ledL = []
            urls = []
            for frame in frames:
                url = frame.url
                urls.append(url)
                try:
                    response = urllib.request.urlopen(url).read().split()
                except Exception:
                    print(url, file=sys.stderr)

                #this is for ftp
                for names in response:
                    if(names.count('ALPSRP') and (not names.count('json'))):# can use re, but this is easier. it selects some files
                        if(names.count('IMG')):
                            imgL.append(names)
                        elif(names.count('LED')):
                            ledL.append(names)

            imgList.append(imgL)
            ledList.append(ledL)
            urlsList.append([urls,imgL])
            urlsList.append([urls,ledL])
        
        retList = [[imgList[0],ledList[0],master],[imgList[1],ledList[1],slave]]
       
        return retList,urlsList

    def retrieveInputFile(self,url,fileIn):
        import time
        prc = PRC()
        t0 = time.time()
        command = 'curl -u ' + prc.dav_u + ':' + prc.dav_p + ' -O ' + os.path.join(url,fileIn)
        os.system(command)
        print((time.time() - t0))
        pass
    def createInputFile(self,inList,project=None):
        ifc = IFC()
        ifc.createInputFile(project,inList)

    def createProductJson(self,dirW):
        jsonProduct = {}
        files = os.listdir(dirW)
        jsonProduct['type'] = self._productType['alos']
        jsonProduct['name'] = self._productName
        fileInList = []
        for fl in files:
            if(fl.count('IMG') and fl.count('ALPSRP')):
                fileInList.append(fl)
        jsonProduct['frameList'] = fileInList
        #this method needs to be called after the corners have been created
        jsonProduct['imageCorners'] = self._imageCorners
        return jsonProduct
    
   
