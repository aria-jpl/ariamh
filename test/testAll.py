#!/usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import os
import sys
import json
import argparse
from httplib2 import Http
from urllib.parse import urlencode
from peg_region_check.PegRegionChecker import PegRegionChecker
from frameMetadata.FrameMetadata import FrameMetadata
from interferogram.PrepareInterferogramCosmo import PrepareInterferogramCosmo as PI
from utils.UrlUtils import UrlUtils
from network_selector.networkSelector import checkPegRegion, checkCoherence
uu = UrlUtils()
rest_url = uu.rest_url
requester = Http()
def findFrame():
    totFound = 0
    for dire in ['asc','dsc']:
        for sc in ['CSKS1','CSKS2','CSKS3','CSKS4']:
            params = {
                'spacecraftName'  : sc,
                'direction':dire,
                'responseGroups': 'Medium'
                }
            resp,cnt = requester.request("%s?%s" %(rest_url,urlencode(params)))
            metList = []
            if(resp['status'] == PegRegionChecker.STATUS_QUERY_OK):
                results = json.loads(cnt)
                if results['result']:
                    for result in results['result']:
                        fm = FrameMetadata()
                        fm.load(result)
                        tbp,peg = checkPegRegion(sc,fm)
                        if( not tbp):
                            exit = 10
                        else:
                            tbpNew,pegNew = checkCoherence(tbp,peg)
                            if( not tbpNew):
                                exit = 10
                            else:
                                return fm.url
#run it as testAll.py [-f h5_frame --frame=h5_frame -t]
# it will run the following steps
# 1) metadata extraction input file creation
# 2) metadata extraction
# 3) network selection
# 4) interferogram creation
# options are all optional
# -f h5_frame --frame=h5_frame specify a frame for which the all process is run, otherwise it will find the first interferable
# frame and  run on it
# -t trim the number of frames to only one frame per set (to test quickly)

def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--frame',help='CSK h5 frame',default = '',dest='f')
    parser.add_argument('-p','--project',help='run processing for this project',default = '',dest='project')
    parser.add_argument('-s','--step',type=int,help='start from specified step',default = 0,dest='step')
    parser.add_argument('-t',help='If set only a single frame pair is used',action= 'store_true',default = False,dest='t')
    opts = parser.parse_args(argv)
    try:
        ARIA_HOME = os.environ['ARIAMH_HOME']
    except:
        ARIA_HOME = os.environ['HOME']
    if opts.f:
        hdf5 = opts.f
    else:
        url = findFrame()
        fileIn = os.path.split(url)[1] + '.tar.gz'
        pi = PI()
        hdf5 = pi.retrieveInputFile(url,fileIn)
    if not os.path.exists('context.json'):
        command = 'echo {\\"id\\":1} > context.json'
        print (command)
        os.system(command)
    step = opts.step
    if(opts.project.count('trigger')):
        os.system('python3 ' + os.path.join(ARIA_HOME,'utils','onFlightCoherenceParams.py -p ' + opts.project + ' -t 180 -d 0.4 -b 400 -c 0.2'))
        #os.system('python3 ' + os.path.join(ARIA_HOME,'utils','onFlightPeg.py -p ' + opts.project + ' -n csks2 -t 74  -s 38.01  -e 37.99  -l -122  -d dsc '))
    if(step == 0):
        os.system('python3 ' + os.path.join(ARIA_HOME,'frameMetadata','inputFileCreator.py CSK ' + hdf5 +  ' dummy.raw'))
        step += 1
    if(step == 1):
        os.system('python3 ' + os.path.join(ARIA_HOME,'frameMetadata','extractMetadata.py extractorInput.xml'))
        step += 1

    metaJson = hdf5 + '.json'
    netSelOutput = 'networkSelectorOut.json'
    jobDescr = "jobDescriptor.json"
    '''
    NOTE: if a dem is provided one needs to add in the top level of descr a key value pair
         'demFile':'someDemFile.xml'
    '''
    descr = {'project':opts.project,'mode':'nominal','workflow':'vanilla_isce',
             'unwrapper':'snaphu','unwrap':True,'posting':20,
             "filterStrength":0.7,
             "createInterferogram":{
                                    "geolist":["topophase.cor","filt_topophase.unw","phsig.cor","los.rdr","filt_topophase.unw.conncomp"],
                                    "productList":['filt_topophase.unw','filt_topophase.unw.conncomp']
                                    },
             "networkSelector":
                                {
                                 "inputFile": metaJson,
                                 "outputFile":netSelOutput
                                 }
            }
    #descr = json.load(open('job_description.json'))
    #descr["networkSelector"]["inputFile"] = metaJson
    #descr["networkSelector"]["outputFile"]  = netSelOutput
    json.dump(descr,open(jobDescr,"w"))
    #clean possible previous run
    if(step == 2):
        os.system('rm -rf networkSelectorOut.json*')
        os.system('python3 ' + os.path.join(ARIA_HOME,'network_selector','networkSelector.py ' + jobDescr))
        step += 1
    listDir = os.listdir('./')
    found = False
    for name in listDir:
        newName = name
        if name.count('networkSelectorOut.json'):
            if(opts.t):
                newName = 'toRun.json'
                fp = open(name)
                met = json.load(fp)
                fp.close()
                for m in met[0]:
                    for s in met[1]:
                        if ( (m['latitudeIndexMin'] >= s['latitudeIndexMin'] - 1) and  (m['latitudeIndexMax'] <= s['latitudeIndexMax'] + 1)):
                            fp = open(newName,'w')
                            json.dump([[m],[s]],fp,indent=4)
                            fp.close()
                            found = True
                            break
            if(found or not opts.t):
                if(step == 3):
                    descr["createInterferogram"]["inputFile"] = newName
                    json.dump(descr,open(jobDescr,"w"))

                    os.system('python3 ' + os.path.join(ARIA_HOME,'interferogram','createInterferogram.py ' + jobDescr))
                    step += 1
           

            break

    


if __name__ == '__main__':
    if(len(sys.argv) > 1):
        argv = sys.argv[1:]
    else:
        argv = ''
    sys.exit(main(argv))
