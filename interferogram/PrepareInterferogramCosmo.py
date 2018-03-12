#!/usr/bin/env python3
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import sys


from interferogram.InputFileCreatorCosmo import InputFileCreatorCosmo as IFC 
import urllib.request, urllib.error, urllib.parse
import json
import os
import re 
import requests
import ssl
from peg_region_check.PegRegionChecker import PegRegionChecker as PRC

class PrepareInterferogramCosmo(object):



    def __init__(self):
        pass
        
    #metalist is a 2 element list (master and slave) and each elememt is a list of contiguous frames.
    #master = output filename of master raw file
    #slave = output filename of slave raw file
    #return a list of filenames and one of urls used to create the input file and localize the input images
    def createInputList(self,metaList,master,slave):
        retList = []
        urlsList = []
        imgList = []
        for frames in metaList:
            imgL = []
            urls = []
            for frame in frames:
                url = frame.url
                urls.append(url)
                response = None
                try:
                    # get the aria-dav url
                    from utils.UrlUtils import UrlUtils
                    uu = UrlUtils()
                    # create a password manager
                    password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()

                    # Add the username and password.
                    password_mgr.add_password(None, uu.dav_url, uu.dav_u, uu.dav_p)

                    handler = urllib.request.HTTPBasicAuthHandler(password_mgr)

                    # create "opener" (OpenerDirector instance)
                    context = ssl._create_unverified_context()
                    opener = urllib.request.build_opener(handler, urllib.request.HTTPSHandler(context=context))

                    # use the opener to fetch a URL
                    response = opener.open(url).read().decode('utf-8')
                except Exception as e:
                    print(e)
                    if response is None:
                        try:
                            r = requests.get(url, auth=(uu.dav_u, uu.dav_p), verify=False)
                            r.raise_for_status()
                            response = r.text
                        except Exception as e:
                            print(e)
                if(response):
                    pattern = 'EL.*?tar.gz'
                    found = re.findall(pattern,response)
                    if(found):
                        imgL.append(found[0])
                    else:
                        print('Expected to have found a EL*.tar.gz file')
                        raise Exception
            imgList.append(imgL)
            urlsList.append([urls,imgL])
        retList = [[imgList[0],master],[imgList[1],slave]]
        return retList,urlsList
    #for cosmos we have a tar files that contains several files.  create a tmpDir, untar in there , move the csk file
    #to the cwd and clean. also return the name of the file
    def retrieveInputFile(self,url,fileIn):
        import urllib.request, urllib.error, urllib.parse
        import os
        import time
        import shutil
        from utils.UrlUtils import UrlUtils
        uu = UrlUtils()
        t0 = time.time()
        command = 'curl -k -u ' + uu.dav_u + ':' + uu.dav_p + ' -O ' + os.path.join(url,fileIn)
        os.system(command)
        tmpDir = 'tmp'
        #not needed but when debugging the directory might already exist
        try:
            shutil.rmtree(tmpDir)
        except:
            pass
        os.mkdir(tmpDir)
        shutil.move(fileIn,tmpDir)
        os.chdir(tmpDir)
        os.system('tar -xzvf ' + fileIn)
        allF = os.listdir('./')
        ret = ''
        for name in allF:
            if name.endswith('.h5'):
                try:
                    shutil.move(name,'../')
                except Exception:
                    #normally is for debugging and the file is already present
                    pass
                os.chdir('../')
                ret = name
                try:
                    shutil.rmtree(tmpDir)
                except:
                    pass
                break

        print((time.time() - t0))
        return ret

    def createInputFile(self,inList,project = None):
        ifc = IFC()
        ifc.createInputFile(inList,project)
        return ifc.getFileOut()
    def createProductJson(self,dirW):
        #TODO 
        return None

def main():
    pi = PrepareInterferogramCosmo()
    pi.retrieveInputFile(1,2)

if __name__ == '__main__':
    sys.exit(main())
