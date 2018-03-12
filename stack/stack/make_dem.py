#!/usr/bin/env python

import numpy
import isce
import os
import sys
import logging
import sarSetup as SS
import argparse

logger = logging.getLogger('DemStitcher')

def createDem(lat=None, lon=None, target='.', buffer=0.):
    '''
    Create the DEM from the bounding box information.

    '''
    from contrib.demUtils.DemStitcher import DemStitcher
    DS = DemStitcher()
    latMin = numpy.floor(numpy.min(lat)-buffer)
    latMax = numpy.ceil(numpy.max(lat)+buffer)
    lonMin = numpy.floor(numpy.min(lon)-buffer)
    lonMax = numpy.ceil(numpy.max(lon)+buffer)

    nsMin,ewMin = DS.convertCoordinateToString(latMin, lonMin)
    nsMax,ewMax = DS.convertCoordinateToString(latMax, lonMax)
    demName = (
        'demLat_' + nsMin + '_' +nsMax +
        '_Lon_' + ewMin +
        '_' + ewMax  + '.dem'
        )
    demName = os.path.join(target, demName)
    demNameXml = demName + '.xml'
    print 'Downloading: ', demName
    DS.setCreateXmlMetadata(True)
    DS.setMetadataFilename(demNameXml)#it adds the .xml automatically

    #if it's already there don't recreate it
    if not (os.path.exists(demNameXml) and os.path.exists(demName)):

        #check whether the user want to just use high res dems and filling the
        # gap or go to the lower res if it cannot complete the region
        # Better way would be to use the union of the dems and doing some
        # resampling
        #try first the best resolution
        DS.setNoFilling()
        source = 1
        stitchOk = DS.stitchDems([latMin, latMax],
                                [lonMin, lonMax],
                                source,
                                demName,
                                keep=False)#remove zip files

        if not stitchOk:#try lower resolution if there are no data
            DS.setFilling()
            source = 3
            stitchOk = DS.stitchDems([latMin, latMax], [lonMin, lonMax],
                                    source, demName, keep=False)

        if not stitchOk:
            logger.error("Cannot form the DEM for the region of interest. If you have one, set the appropriate DEM component in the input file.")
            raise Exception

    #if stitching is performed a DEM image instance is created (returns None otherwise). If not we have to create one
    demImage = DS.getImage()
    return demName


def constructDem(source='.', target='.', buffer=0.):
    '''
    Uses the h5 files in source directory to determine bounding box and creates
    an appropriate DEM in the target directory.
    '''

    flist = SS.geth5names(dirname=source)
    lat, lon = SS.getGeoLimits(flist = flist)

    dname = createDem(lat=lat, lon=lon, target=target, buffer=buffer)
    return dname

def parse():
    '''Command line parser.'''
    parser = argparse.ArgumentParser(description='Create a DEM based on metadata from a h5 dir.')
    parser.add_argument('-i', action='store', default='.', dest='srcDir', 
            help='srcDir with h5 files.', type=str)
    parser.add_argument('-o', action='store', default='.', dest='tarDir',
            help='target directory in which the DEM is downloaded to.', type=str)
    parser.add_argument('-buffer', action='store', default=0.05, dest='buffer',
            help='Padding around the bbox of image obtained from metadata')
    inps = parser.parse_args()

    return inps

if __name__ == '__main__':
    '''
    Create the required DEM.
    '''
    inps = parse()
    constructDem(source=inps.srcDir, target=inps.tarDir, buffer=inps.buffer)

