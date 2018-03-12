#!/usr/bin/env python3

import isce
import numpy as np 
import h5py
import argparse
import os

def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser(description='Extract velocity field from Timefn analysis')
    parser.add_argument('-i', '--input', dest='h5file', type=str, default='Stack/TS-PARAMS.h5',
            help='Timefn HDF5 file')
    parser.add_argument('-o', '--output', dest='velfile', type=str, default='LOS_velocity.geo')
    parser.add_argument('-x', '--xml', dest='xmlfile', type=str, required=True,
            help='Example InsarProc.xml file to geocoding information from')

    return parser.parse_args()


def getVelocity(h5file, outfile):
    '''
    Extract velocity from h5file.
    '''

    fid = h5py.File(h5file, 'r')
    parms = fid['parms']
    vel = parms[:,:,1].astype(np.float32)
    shape = vel.shape
    vel.tofile(outfile)
    fid.close()
    return shape


def copyGeoXML(xmlfile, outfile, shape):
    '''
    Copy Geocoding information from xmlfile to outfile.xml.
    '''
    import isceobj

    img = isceobj.createDemImage()
    img.load(xmlfile)

    img.setDataType('FLOAT')
    img.setBands(1)
    img.setAccessMode('READ')
    img.setInterleavedScheme('BIL')
    img.setFilename(outfile)

    ####Not needed if no cropping is performed.
    img.setWidth(shape[1])
    img.setLength(shape[0])

    img.addDescription('LOS velocity in mm/yr')
    img.renderHdr()

    return

if __name__ == '__main__':
    '''
    Main driver.
    '''

    ###Parse command line
    inps = cmdLineParse()

    shape = getVelocity(inps.h5file, inps.velfile)
    
    copyGeoXML(inps.xmlfile, inps.velfile, shape)


