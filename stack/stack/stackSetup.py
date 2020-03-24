#!/usr/bin/env python3

from __future__ import absolute_import
from builtins import str
from builtins import object
import numpy
from . import sarSetup as SS
from . import insarSetup as IS
from . import insar_check as IC
import os 
import sys
import argparse

class h5Stack(object):
    '''Alternative to inps for insar_check'''
    def __init__(self,flist, params=None):

        try:
            self.plot = params.plot
        except:
            self.plot = False

        try:
            self.Bcrit = params.Bcrit
        except:
            self.Bcrit = 400.0
        
        try:
            self.Tau = params.Tau
        except:
            self.Tau = 180.0

        try:
            self.dop = params.dop
        except:
            self.dop = 0.4

        try:
            self.cThresh = params.cThresh
        except:
            self.cThresh = 0.3

        if isinstance(flist,str):
            flist = [flist]

        self.fnames = flist
        self.raw = SS.isRaw(flist[0])

def pairDirs_from_file(infile, base=''):
    '''Read a text file and generate list of pairDirs.'''

    pairDirs = []
    data = numpy.atleast_2d(numpy.atleast_2d(numpy.loadtxt(infile, dtype='S')).astype('|U'))
    if (data.shape[0] < data.shape[1]) and ('_' in data[0,0]):
        data = data.T

    for line in data:
        if len(line) == 1:
            pairDirs.append(os.path.join(base,str(line[0])))
        elif len(line) == 2:
            pairDirs.append(os.path.join(base, '_'.join(
                [str(line[0]), str(line[1])])))

    return pairDirs

def prepIfgList(source='.', target='.', listfile=False, params=None):
    '''Creates and IFG list with the dates.'''
    
    flist = []

    for dirname in os.listdir(source):
        dname = os.path.join(source, dirname)
        if os.path.isdir(dname) and dirname.startswith('2'):
            ret = SS.geth5names(dname)
            #Pick the average frame
            flist.append(ret[len(ret)//2])

    stackObj = h5Stack(flist, params=params)
    pairList = IC.process(stackObj)

    if listfile:
        fid = open(os.path.join(target , 'ifg.list'),'w')
        for ifg in pairList:
            fid.write('{0}  {1}  {2}  CSK \n'.format(ifg[0], ifg[1], ifg[2]))

        fid.close()
    return pairList

def prepareDirs(pairList, source='.', target = '.',
        dem=None, force=False):
    '''
    Create the required directory structure and the
    corresponding XML files.
    '''

    currdir = os.getcwd()
    for pair in pairList:
        master = pair[0]
        slave = pair[1]
        pairDir = os.path.join(target, '{0}_{1}'.format(master,slave))
        exists = os.path.isdir(pairDir)
        if not exists:
            os.mkdir(pairDir)

        os.chdir(pairDir)

        if (not exists) or force:
            #Set up master catalog
            relPath = os.path.relpath(os.path.join(source, master), pairDir)
            f1 = SS.geth5names(relPath)
            mxml = SS.sarCatalogXML(f1)

            #Set up slave catalog
            relPath = os.path.relpath(os.path.join(source, slave), pairDir)
            f2 = SS.geth5names(relPath)
            sxml = SS.sarCatalogXML(f2)

            if dem not in [None,'']:
                demN = os.path.relpath(dem, pairDir)
                if not demN.endswith('.xml'):
                    demN += '.xml'
            else:
                demN = None
        
            IS.makeXML(master=mxml, slave=sxml, dem=demN, raw=SS.isRaw(f1[0]))

        os.chdir(currdir)
    return

def getPairDirs(dirname='.', fullpath=True):
    '''
    Returns list of directories corresponding to
    intereferograms in the given dir.
    '''
    dlist = []

    for kk in os.listdir(dirname):
        if os.path.isdir(os.path.join(dirname, kk)):
            if kk.startswith('2') and len(kk.split('_'))==2:
                if fullpath:
                    dlist.append(os.path.join(dirname, kk))
                else:
                    dlist.append(kk)

    return dlist

def parse():
    '''Command line parser.'''
    parser = argparse.ArgumentParser(description='Prepare stack directories')
    parser.add_argument('-i', action='store', default='.', dest='srcDir',
            help='Directory with h5 files organized as subdirs.', type=str)
    parser.add_argument('-o', action='store', default='.', dest='tarDir',
            help='Directory for setting up IFGs as subdirs.', type=str)
    parser.add_argument('-d', action='store', default=None, dest='dem',
            help='DEM file to use for IFGs.', type=str)
    parser.add_argument('-noprep', action='store_true', default=False,
            dest='noprep', help='Print list of IFGs only.')
    parser.add_argument('-ilist', action='store', default='', dest='ilist',
            help='List with predefined pairs.', type=str)
    parser.add_argument('-force', action='store_true', default=False,
            dest='force', help='Force creating of directories.')
    parser.add_argument('-Bcrit', action='store', default=400.,
            dest='Bcrit', help='Critical baseline', type=float)
    parser.add_argument('-Tau', action='store', default=180.,
            dest='Tau', help='Temporal decorrelation coefficient.', type=float)
    parser.add_argument('-dop', action='store', default=0.4,
            dest='dop', help='Doppler difference in fraction of PRF', type=float)
    parser.add_argument('-coh', action='store', default=0.3,
            dest='cThresh', help='Coherence threshold', type=float)
    parser.add_argument('-plot', action='store_true', default=False,
            dest='plot', help='Show Baseline plot.')
    inps = parser.parse_args()
    return inps

if __name__ == '__main__':
    '''Prepares the list of IFGs to proces.'''

    inps = parse()

    if inps.ilist in ['', None]:
        pairs = prepIfgList(source=inps.srcDir, target=inps.tarDir, params=inps)
    else:
        pairtxt = pairDirs_from_file(inps.ilist)
        pairs = []
        for kk in pairtxt:
            temp = kk.split('_')
            pairs.append([temp[0], temp[1], 0.0])

    if not inps.noprep:
        print('Preparing directories for {0} IFGS'.format(len(pairs)))
        prepareDirs(pairs, source=inps.srcDir,
                target=inps.tarDir, dem=inps.dem, force=inps.force)
