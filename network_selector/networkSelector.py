#! /usr/bin/env python3
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2011, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


import pdb
import os
import sys
import json
import traceback
from os import path
from frameMetadata.FrameMetadata import FrameMetadata
from peg_region_check.PegReader import PegReader, PegInfoFactory
from peg_region_check.PegRegionChecker import PegRegionChecker
from frameMetadata.OrbitInfo import OrbitInfo
import argparse
from iscesys.Compatibility import Compatibility
Compatibility.checkPythonVersion()
from utils.contextUtils import toContext
from network_selector.coherenceValues import getParameters 
#inputs
#argv[0] is the json file with the frame metadata
#argv[1] name of the output pickle file with the results

# outputs and description
# if network finds something it saves the results in outputFile.
# the result is a pickle file that contains a tuple. the first element is a list of lists of lists
# each element is a pair of lists and  each element of the pair is a list of frames object that are interferable with the 
# other pair element. They will need to be stitched
# example 
# [
# [[frame111,frame112...,frame11M],[frame121,frame122...,frame12M]],
# [[frame211,frame212...,frame21M],[frame221,frame222...,frame22M]],
# ......
# [[frameN11,frameN12...,frameN1M],[frameN21,frameN22...,frameN2M]],
# ]
# each set of frameij[1...M] will be stitched together
# 
# return 0 if finds something, 10 if no go, 255 if any crash occurred



def checkPegRegion(fm,project):
    prc = PegRegionChecker(fm,project)
    return prc.runNominalMode()

def checkCoherence(tbp,peg,project):
    isCoherent = []
    bCrit,tau,doppler,thr = getParameters(project)
    for pairs in tbp:
        isCoh = True
        for fm1,fm2 in zip(pairs[0],pairs[1]):
            oi = OrbitInfo(fm1)
            if not (oi.isCoherent(fm2,bCrit,tau,doppler,thr)):
                isCoh = False
                break
        isCoherent.append(isCoh)
    tbpNew = []
    pegNew = []
    for pairs,pg,coh in zip(tbp,peg,isCoherent):
        if coh:
            tbpNew.append(pairs)
            pegNew.append(pg)
    tbpSort = []
    for pairs in tbpNew:
        msSort = []
        for ms in pairs:
            msSort.append(sorted(ms ,key=lambda x: x._sensingStart))
        tbpSort.append([msSort[0],msSort[1]] if msSort[0][0]._sensingStart > msSort[1][0]._sensingStart else [msSort[1],msSort[0]] )

    tbpNew = tbpSort
    return tbpNew,pegNew

def toDict(pairs):
    ret = []
    for pair in pairs:
        frames = []
        for frame in pair:
            frames.append(frame.toDict())
        ret.append(frames)
    return  ret
            
#inputs
# argv[0]: json file containing the metadata
# argv[1]: prefix of the output files. a '_x' x = integer is appended to discriminate
# there will be an output file per each  interferable pair of frames covering a certain peg region

# return 0 = success i.e. found interferable pairs, 10 = peg region not covered, 11 = no coherence, 255 something went worng


def main():
    import json
    inputs = json.load(open(sys.argv[1]))
    '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-i','--input',dest='input',type=str,help='Input json filename containing metadata')
    parser.add_argument('-o','--output',dest='output',type=str,help='Output prefix name for the results') 
    parser.add_argument('-p','--project',dest='project',type=str,help='Project that belongs too')                                                                    
    args = parser.parse_args()
    '''                                                             
    process = 'networkSelector'
    message = ''

    exitv = 0 
    try:       
        outputFile = inputs['networkSelector']['outputFile']
        with open(inputs['networkSelector']['inputFile']) as fp:
            meta = json.load(fp)
        fm = FrameMetadata()
        fm.load(meta)
        sensor = fm.getSpacecraftName()
        tbp,peg = checkPegRegion(fm,inputs['project'])
        message = 'Found complete PEG region'
        if(not tbp):
            exitv = 10
            message = 'Not found complete PEG region'

        else:
            tbpNew,pegNew = checkCoherence(tbp,peg,inputs['project'])
            if(not tbpNew):
                exitv = 11
                message = 'Coherence below threshold'
            else:
                for i in range(len(tbpNew)):
                    with open(outputFile + '_' + str(i),'w') as fp:
                        json.dump(toDict(tbpNew[i]),fp,indent=4)
    except Exception as e:
        exitv = 255 
        message = 'Failed with exception %s: %s' % (str(e), traceback.format_exc())
        toContext(process,exitv,message)
    
    toContext(process,exitv,message)
    return exitv


if __name__ == "__main__":
    sys.exit(main())
