#!/usr/bin/env python

import argparse
import os
import sys
import logging
import lxml.objectify as OB
import numpy as np
from numpy.lib.stride_tricks import as_strided
from collections import OrderedDict
import cPickle


errorCodes = {
                'GMT Error' : 10,
                'File Read Error' : 20,
                'File Writer Error' : 30,
                'GPS Data Error' : 40,
                'Not enough GPS points' : 50,
                'Not enough coherence' : 60,
             }



##########Classes and utils for memory maps
class memmap(object):
    '''Create the memap object.'''
    def __init__(self,fname, mode='readonly', nchannels=1, nxx=None, nyy=None, scheme='BSQ', dataType='f'):
        '''Init function.'''

        fsize = np.zeros(1, dtype=dataType).itemsize

        if nxx is None:
            raise ValueError('Undefined file width for : %s'%(fname))

        if mode=='write':
            if nyy is None:
                raise ValueError('Undefined file length for opening file: %s in write mode.'%(fname))
        else:
            try:
                nbytes = os.path.getsize(fname)
            except:
                raise ValueError('Non-existent file : %s'%(fname))

            if nyy is None:
                nyy = nbytes/(fsize*nchannels*nxx)

                if (nxx*nyy*fsize*nchannels) != nbytes:
                    raise ValueError('File size mismatch for %s. Fractional number of lines'(fname))
            elif (nxx*nyy*fsize*nchannels) > nbytes:
                    raise ValueError('File size mismatch for %s. Number of bytes expected: %d'%(nbytes))
             

        self.name = fname
        self.width = nxx
        self.length = nyy

        ####List of memmap objects
        acc = []

        ####Create the memmap for the full file
        nshape = nchannels*nyy*nxx
        omap = np.memmap(fname, dtype=dataType, mode=mode, 
                shape = (nshape,))

        if scheme.upper() == 'BIL':
            nstrides = (nchannels*nxx*fsize, fsize)

            for band in range(nchannels):
                ###Starting offset
                noffset = band*nxx

                ###Temporary view
                tmap = omap[noffset:]

                ####Trick it into creating a 2D array
                fmap = as_strided(tmap, shape=(nyy,nxx), strides=nstrides)

                ###Add to list of objects
                acc.append(fmap)

        elif scheme.upper() == 'BSQ':
            nstrides = (fsize, fsize)

            for band in range(nchannels):
                ###Starting offset
                noffset = band*nxx*nyy

                ###Temporary view
                tmap = omap[noffset:noffset+nxx*nyy]

                ####Reshape into 2D array
                fmap = as_strided(tmap, shape=(nyy,nxx))

                ###Add to lits of objects
                acc.append(fmap)

        elif scheme.upper() == 'BIP':
            nstrides = (nchannels*nxx*fsize,nchannels*fsize)

            for band in range(nchannels):
                ####Starting offset
                noffset = band

                ####Temporary view
                tmap = omap[noffset:]

                ####Trick it into interpreting ot as a 2D array
                fmap = as_strided(tmap, shape=(nyy,nxx), strides=nstrides)

                ####Add to the list of objects
                acc.append(fmap)

        else:
            raise ValueError('Unknown file scheme: %s for file %s'%(scheme,fname))

        ######Assigning list of objects to self.bands
        self.bands = acc




def getDirList(insarDir):
    '''
    Get a list of staged interferogram directories.
    '''
    dirList = []
    for root, dirs, files in os.walk(insarDir):
        for dir in dirs:
            if len(dir.split('_')) == 2:
                dirList.append(os.path.join(insarDir, dir))

    return dirList

def uniqueList(seq):
        '''
        Returns a list with unique elements in a list.
        '''
        seen = set()
        seen_add = seen.add
        return [ str(x) for x in seq if x not in seen and not seen_add(x)]

def getDatesFromIntList(intList):
    '''
    Get a list of acquisition dates from list of int directories.
    '''
    dateList = []

    for pair in intList:
        dateList += os.path.basename(pair).split('_')

    return sorted(uniqueList(dateList), key=str.lower)


def getGeoData(insarXml):
    '''
    Get dimensions of the image and location from insarXML.
    '''
    try:
        fid = open(insarXml, 'r')
        xObj = OB.fromstring(fid.read())
        fid.close()
    except:
        print 'Cannot read metadata from : %s'%(insarXml)
        sys.exit(errorCodes['File Read Error'])

    rdict = {}
    rdict['width'] = int(xObj.runGeocode.outputs.GEO_WIDTH)
    rdict['length'] = int(xObj.runGeocode.outputs.GEO_LENGTH)
    rdict['heading'] = float(xObj.runGeocode.inputs.PEG_HEADING) * 180.0 / np.pi
    rdict['wvl'] = float(xObj.runGeocode.inputs.RADAR_WAVELENGTH)
    rdict['deltarg'] = 30.
    rdict['deltaaz'] = 30.
    rdict['deltaLat'] = float(xObj.runGeocode.outputs.LATITUDE_SPACING)
    rdict['deltaLon'] = float(xObj.runGeocode.outputs.LONGITUDE_SPACING)

    #####Get Lat / Lon information
    maxLat = float(xObj.runGeocode.outputs.MINIMUM_GEO_LATITUDE)
    minLat = float(xObj.runGeocode.outputs.MAXIMUM_GEO_LATITUDE)
    minLon = float(xObj.runGeocode.outputs.MINIMUM_GEO_LONGITUDE)
    maxLon = float(xObj.runGeocode.outputs.MAXIMUM_GEO_LONGITUDE)
    rdict['snwe'] = [minLat,maxLat,minLon,maxLon]
    return rdict


def getImageData(fileXml):
    '''
    Get data from image XML file.
    '''
    try: 
        fid = open(fileXml, 'r')
        xObj = OB.fromstring(fid.read())
        fid.close()
    except:
        print 'Cannot read metadata from : %s'%(fileXml)
        sys.exit(errorCodes['File Read error'])

    prop = xObj.findall('property')
    rdict = {}
    for kk in prop:
        rdict[kk.attrib['name']] = kk.value.text

    return rdict

def createMemmap(filename, datatype='f'):
    '''
    Create a memmap from ISCE file.
    '''

    if filename.endswith('.xml'):
        xmlfile = filename
        datafile = os.path.splitext(filename)[0]
    else:
        datafile = filename 
        xmlfile = filename + '.xml'


    rdict = getImageData(xmlfile)
    mObj = memmap(datafile, nchannels= int(rdict['NUMBER_BANDS']),
                  nxx = int(rdict['WIDTH']), nyy=int(rdict['LENGTH']),
                  scheme = rdict['SCHEME'], dataType = datatype)


    return mObj

