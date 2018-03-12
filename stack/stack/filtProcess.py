#!/usr/bin/env python

####This script will include atmospheric corrections code in the future.
####Currently not used.
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Piyush Agram
# Copyright 2013, by the California Institute of Technology. ALL RIGHTS
# RESERVED. United States Government Sponsorship acknowledged.
# Any commercial use must be negotiated with the Office of Technology Transfer
# at the California Institute of Technology.
#
# This software may be subject to U.S. export control laws. By accepting this
# software, the user agrees to comply with all applicable U.S.
# export laws and regulations. User has the responsibility to obtain export
# licenses, or other export authority as may be required before 
# exporting such information to foreign countries or providing access to
# foreign persons.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import isce
import stdproc
import isceobj
import argparse
import os
import stackSetup as SS

def load_pickle(step='filter'):
    import cPickle

    insarObj = cPickle.load(open('PICKLE/{0}'.format(step), 'rb'))
    return insarObj

def estPhaseSigma(insar):
    from mroipac.icu.Icu import Icu

    intImage = isceobj.createSlcImage()     #Filtered file
    intImage.setWidth(insar.resampIntImage.width)
    intImage.setFilename(insar.topophaseFlatFilename)
    intImage.setAccessMode('read')

    ampImage = isceobj.createAmpImage()
    ampImage.setWidth(insar.resampIntImage.width)
    ampImage.setFilename('resampOnlyImage.amp')
    ampImage.setAccessMode('read')


    outImage = isceobj.createRgImage()
    outImage.imageType = 'cor'
    outImage.scheme = 'BIL'
    outImage.bands = 1
    outImage.setWidth(insar.resampIntImage.width)
    outImage.setFilename('phsig.cor')
    outImage.setAccessMode('write')


    intImage.createImage()
    ampImage.createImage()
    outImage.createImage()

    icuObj = Icu()
    icuObj.filteringFlag=False
    icuObj.unwrappingFlag = False
    icuObj.initCorrThreshold = 0.1

    icuObj.icu(intImage=intImage, ampImage=ampImage,phsigImage=outImage)

    outImage.renderHdr()
    intImage.finalizeImage()
    ampImage.finalizeImage()
    outImage.finalizeImage()
 
def parse():
    '''
    Command line parser.
    '''
    parser = argparse.ArgumentParser(description='Process and correct filtered interferograms.')

    parser.add_argument('-d', action='store', default='.',
        dest='dirname', help='Directory with prepared interferogram dirs.',
        type=str)
    parser.add_argument('-force', action='store_true', default=False,
        dest='force', help='Force reprocessing.')
    parser.add_argument('-l', action='store', default='',
        dest='subset', help='Consider only a subset of dirs given in file.',
        type=str)
    parser.add_argument('--pyaps', action='store_true', default=False,
            dest='pyaps', help='Use pyaps for atmospheric corrections.')
    parser.add_argument('--tropo', action='store_true', default=False,
            dest='tropo', help="Use Angie's tropomap for corrections.")
    parser.add_argument('--atmosdir', action='store', default='.',
            dest='atmosdir', type=str, 
            help='Directory for storing atmospheric correction data.')

    inps = parser.parse_args()
    
    ####Check atmospheric flags
    if inps.pyaps and inps.tropo:
        raise ValueError('Two options for atmospheric corrections provided.')

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

    if inps.pyaps:
        print 'PyAPS corrections desired. '
        print 'PyAPS data to be stored in {0}'.format(inps.atmosdir)
    elif inps.tropo:
        print 'Tropo corrections desired. '
        print 'Tropo data to be stored in {0}'.format(inps.atmosdir)

    for pair in pairDirs:
        corfile = os.path.join(pair, 'phsig.cor')
        if os.path.exists(corfile) and (not inps.force):
            print 'Interferogram {0} already processed'.format(pair)
        else:
            print 'Processing: {0}'.format(pair)

            ####Estimate phase standard deviation
            os.chdir(pair)
            iobj = load_pickle()
            estPhaseSigma(iobj)

            ####Estimate PyAPS corrections
            if inps.pyaps:
                print 'PyAPS functions will go here.'

            if inps.tropo:
                print 'Tropo functions will go here.'

            os.chdir(currDir)
