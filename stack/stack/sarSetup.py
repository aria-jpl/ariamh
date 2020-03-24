#!/usr/bin/env python3

from __future__ import absolute_import
from builtins import str
import os
import h5py
from . import xmlSetup as XS
import numpy

def isRaw(fname):
    '''
    Returns if a given h5 file corresponds to RAW / SLC.
    '''
    return ('RAW' in fname)

def geth5names(dirname='.'):
    '''
    Returns the h5files in a given directory.
    '''

    flist = []

    for filename in os.listdir(dirname):
        if filename.endswith('.h5'):
            flist.append(os.path.join(dirname,filename))
    
    return flist

def getDateFromh5(fname):
    '''Get acquisition date from CSK h5 file name.'''

    g = fname.split('_')[-1]
    datestr = g[0:8]
    return datestr

def getBboxFromh5(fname):
    '''Get the bounding box from a h5 file.'''

    fid = h5py.File(fname, 'r')

    locs = numpy.zeros((4,3), dtype=numpy.float)
    if isRaw(fname):
        locs[0,:] = fid.attrs['Estimated Bottom Left Geodetic Coordinates']
        locs[1,:] = fid.attrs['Estimated Bottom Right Geodetic Coordinates']
        locs[2,:] = fid.attrs['Estimated Top Right Geodetic Coordinates']
        locs[3,:] = fid.attrs['Estimated Top Left Geodetic Coordinates']
    else:
        locs[0,:] = fid['S01/SBI'].attrs['Bottom Left Geodetic Coordinates']
        locs[1,:] = fid['S01/SBI'].attrs['Bottom Right Geodetic Coordinates']
        locs[2,:] = fid['S01/SBI'].attrs['Top Right Geodetic Coordinates']
        locs[3,:] = fid['S01/SBI'].attrs['Top Left Geodetic Coordinates']

    fid.close()

    lat = [numpy.min(locs[:,0]), numpy.max(locs[:,0])]
    lon = [numpy.min(locs[:,1]), numpy.max(locs[:,1])]
    return lat, lon

def getGeoLimits(flist =None, dirname = '.'):
    '''Get bbox from h5 files in a directory.'''

    if flist is None:
        flist = geth5names(dirname)

    latList = []
    lonList = []
    for kk in flist:
        lat, lon = getBboxFromh5(kk)
        latList += lat
        lonList += lon

    
    lat = [numpy.min(latList), numpy.max(latList)]
    lon = [numpy.min(lonList), numpy.max(lonList)]

    return lat, lon

def sarCatalogXML(flist, filename=None):
    '''
    Creates a simple catalog file to be used with insarApp or make_raw.
    '''

    sdict = {}
    if len(flist) > 1:
        sdict['HDF5'] = str(flist)
    else:
        sdict['HDF5'] = str(flist[0])

    sdict['OUTPUT'] = getDateFromh5(flist[0])+'.raw'

    if filename is None:
        filename  = getDateFromh5(flist[0])+'.xml'

    fid = open(filename, 'w')
    root = XS.XMLFromDict(sdict, name = 'sar')
    XS.writeXML(fid, root)
    fid.close()

    return filename


if __name__ == '__main__':
    flist = geth5names(dirname= '../h5/20130531')
    sarCatalogXML(flist)
