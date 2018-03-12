#!/usr/bin/env python

import sys
import os
import numpy as np
import veloLib
import json
"""
This script 
    - Looks at spatial coverage of all interferograms
    - Sets up valid list
"""
def cmdLineParse():
    '''
    Command line parser.
    '''
    '''
    parser = argparse.ArgumentParser(description='Setup auxiliary data for GIAnT processing')
    parser.add_argument('--dir', action='store', default='./insar',
            type=str, dest='insarDir',
            help = 'Directory where all the interfeograms have been staged.')
    parser.add_argument('--minifg', action='store', default=25,
            type=int, dest='minifg',
            help='Minimum number of interferograms need to proceed. Default:25')
    parser.add_argument('--sigma', action='store', default=2,
            type=float, dest='sigma',
            help='Number of std devs in spatial coverage thats acceptable. Default: 2')
    parser.add_argument('--cthresh', action='store', default=0.3,
            type=float, dest='cthresh',
            help='Coherence threshold for mask.')
    parser.add_argument('--mincov', action='store', default=0.2,
            type=float, dest='mincov',
            help='Minimum coverage need to proceed. Default: 0.2')
    parser.add_argument('--margin', action='store', default=0.1,
            type=float, dest='margin',
            help='Acceptable reduction in coverage even if it lies outside n sigma. Default: 0.1')
    parser.add_argument('--out', action='store', default='valid.list',
            type=str, dest='out',
            help='List of useful interferograms to consider for further processing.')
    parser.add_argument('--common', action='store', default=0.6,
            type=float, dest='common',
            help='Minimum common overlap with most scenes. Default: 0.6')
    '''
    return json.load(open(sys.argv[1]))
    #return parser.parse_args()


if __name__ == '__main__':
    '''
    The main driver for creating aux data.
    '''
    #####Parse command line
    inps = cmdLineParse()

    ####Get current dir
    currDir = os.getcwd()

    #####Get path to one insarProc.xml
    intList = veloLib.getDirList(inps['insarDir'])
    metaData = veloLib.getGeoData(os.path.join(intList[0], 'insarProc.xml'))

    nIfg = len(intList)
    
    #####Track two types of coverage
    cover = np.zeros(nIfg)
    usefulPair = np.ones(nIfg, dtype=np.bool)
    commonMask = np.zeros((metaData['length'], metaData['width']))

    size = metaData['width']*metaData['length']

    ######First Pass over interferograms
    for ind, intdir in enumerate(intList):

        print 'Processing IFG  %d out of  %d'%(ind+1, nIfg)
        
        unwname = os.path.join(intdir,inps['unwFile'])
        corname = os.path.join(intdir,inps['corFile'])
        if not os.path.exists(unwname) or not os.path.exists(corname):
            usefulPair[ind] = False
            print 'Pair %d not useful. skipping ....'%(ind)
            continue

        unw = veloLib.createMemmap(unwname)
        cor = veloLib.createMemmap(corname)

        dmask = (unw.bands[0] != 0)
        cmask = (cor.bands[0] > inps['cthresh_qa'])

        mask = dmask*cmask
        commonMask += mask 

        cover[ind] = np.sum(mask) / (1.0*size)
        unw = None
        cor = None


    #######Apply coverage filter
    for kk in xrange(nIfg):
        if cover[kk] < inps['mincov']:
            usefulPair[kk] = False

    useful = np.sum(usefulPair)
    if useful < inps['minifg']:
        print 'Ifg, coverage'
        for kk in xrange(nIfg):
            print intList[kk], cover[kk]
        raise Exception('Not enough useful pairs left')

    else:
        print '%d Interferograms have coverage greater than min threshold of %f'%(useful, inps['mincov'])



    ######Compare statistics
    meancov = np.median(cover[usefulPair])
    stdcov = np.std(cover[usefulPair])

    mincov = meancov - inps['sigma']*stdcov
    mincov = min(mincov, (1-inps['mincov'])*meancov)

    for kk in xrange(nIfg):
        if (cover[kk] < mincov): 
            usefulPair[kk] = False


    useful = np.sum(usefulPair)
    if useful < inps['minifg']:
        print 'Ifg, coverage'
        for kk in xrange(nIfg):
            print intList[kk], cover[kk]
        raise Exception('Not enough useful pairs left')

    else:
        print '%d Interferograms have coverage that satisfies all conditions'%(useful)


    if np.max(commonMask) != nIfg:
        print 'WARNING !!!!!!!!!!'
        print 'There may be no common region'


    commonMask = (commonMask > inps['minifg'])
    csum = np.sum(commonMask)/(1.0*size)

    print('Common sum: ', csum)
    if (csum < inps['mincov']):
        raise Exception('Not enough common regions between IFGS.')

    commonMask1 = np.ones((metaData['length'], metaData['width']))

    ######Second Pass over interferograms
    for ind, intdir in enumerate(intList):

        print 'Processing IFG  %d out of  %d'%(ind+1, nIfg)
        
        unwname = os.path.join(intdir, inps['unwFile'])
        corname = os.path.join(intdir, inps['corFile'])
        if not os.path.exists(unwname) or not os.path.exists(corname):
            usefulPair[ind] = False
            print 'Pair %d not useful. skipping ....'%(ind)
            continue

        unw = veloLib.createMemmap(unwname)
        cor = veloLib.createMemmap(unwname)

        size = cor.bands[0].size
        dmask = (unw.bands[0] != 0)
        cmask = (cor.bands[0] > inps['cthresh_qa'])
        mask = dmask*cmask
                
        cmask1 = (cor.bands[0] > inps['chthresh'])
        commonMask1 *= dmask*cmask1

        frac = np.sum(mask * commonMask) / (csum * size)
        if (frac < inps['common']):
            usefulPair[kk] = False


    useful = np.sum(usefulPair)
    if useful < inps['minifg']:
        print 'Ifg, coverage'
        for kk in xrange(nIfg):
            print intList[kk], cover[kk]
        raise Exception('Not enough useful pairs left')

    else:
        print '%d Interferograms have coverage that satisfies all conditions'%(useful)

    commonMask1.tofile(inps['qamaskName'])
    ######Print out the valid list
    fid = open(inps['list'], 'w')
    for ind, intdir in enumerate(intList):
        if usefulPair[ind]:
            fid.write("{0}\n".format(os.path.basename(intdir)))

    fid.close()
