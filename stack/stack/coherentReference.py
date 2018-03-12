#!/usr/bin/env python 

import os 
import numpy as np 
import sys
import argparse
import stackSetup as SS
import lxml.objectify as OB

def parse():
    parser = argparse.ArgumentParser(description='Determine a good reference region for time-series estimation')

    parser.add_argument('-d', action='store', default='.', 
        dest='dirname', help='Directory with prepared interferogram dirs.',
        type=str)
    parser.add_argument('-o', action='store', required=True,
        dest='output', help='Output file with reference region information.')
    parser.add_argument('-l', action='store', default='',
        dest='subset', help='Consider only a subset of dirs given in file.',
        type=str)
    parser.add_argument('-win', help='Window size for reference region.',
        default=5, type=int, dest='win')

    inps = parser.parse_args()
    return inps

if __name__ == '__main__':
    '''
    The main driver.
    '''
    inps = parse()

    currDir = os.getcwd()

    if inps.subset in ['', None]:
        pairDirs = SS.getPairDirs(dirname=inps.dirname)
    else:
        pairDirs = SS.pairDirs_from_file(inps.subset, base=inps.dirname)

    print 'Number of IFGs : ', len(pairDirs)

    #####Keep track of pixel-by-pixel coherence flag
    numCoh = None

    #####Keep track of coherent pixels in window around each pixel
    numWinCoh = None 

    #####Dimensions
    width = None
    length = None
    yfirst = None
    xfirst = None
    ystep = None
    xstep = None

    for pair in pairDirs:
        corfile = os.path.join(pair, 'phsig.cor.geo')
        if not os.path.exists(unwfile):
            raise ValueError('Coherence file not found: ' + corfile)

        #######Initialize values if not read in yet
        if (numCoh is None) or (numWinCoh is None):
            fid = open(os.path.join(pair,'insarProc.xml'), 'r')
            xObj = OB.fromstring(fid.read())
            fid.close()
            meta = xObj.runGeocode.outputs
            width = meta.GEO_WIDTH
            length = meta.GEO_LENGTH
            yfirst = meta.MINIMUM_GEO_LATITUDE
            xfirst = meta.MINIMUM_GEO_LONGITUDE
            ystep = meta.LATITUDE_SPACING
            xstep = meta.LONGITUDE_SPACING

            numCoh = np.zeros((length,width), dtype=np.int16)
            numWinCoh = np.zeros((length,width), dtype=np.int16)

	




