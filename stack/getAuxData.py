#!/usr/bin/env python

import os
import sys
import numpy as np
import veloLib
import GPSlib
from collections import OrderedDict
import json
"""
This script 
    - Creates a land water mask
    - Downloads the GPS data for the current dataset
    - Sets up reference region
"""

errorCodes = {
                'GMT Error' : 10,
                'File Read Error' : 20,
                'File Writer Error' : 30,
                'GPS Data Error' : 40,
                'Not enough GPS points' : 50,
                'Not enough coherence' : 60,
             }


def getLandWaterMask(snwe,shape, outFile='mask.flt'):
    '''
    Creates a float32 land water mask and dumps it to the outputFile.
    '''

    tempName = os.path.basename(os.tempnam())+'.grd'
        
    ####GMT land water mask command
    cmd = 'grdlandmask -G{grd} -I{width}+/{length}+ -R{west}/{east}/{south}/{north} -Df'.format(grd=tempName, width=shape[1], length=shape[0],
            west=snwe[2], east=snwe[3], south=snwe[0], north=snwe[1])

    try:
        os.system(cmd)
    except:
        print 'GRDLANDMASK error'
        sys.exit(errorCodes['GMT Error'])

    ##### Dump land mask to a float32 files
    cmd = 'grd2xyz {grd} -ZTLf > {out}'.format(grd=tempName,
                                                out=outFile)
    try:
        os.system(cmd)
    except:
        print 'GRD2XYZ error'
        sys.exit(errorCodes['GMT Error'])

    if os.path.exists(tempName):
        os.remove(tempName)

    return

def createGIAnTGPSrepo(gpsData, lname='stationlist', gpsdir='neu'):
    '''
    Creates files and directory needed by GIAnT in SOPAC format.
    '''
###Create GPS station list
    fid = open(lname, 'w')
    for key,stn in gpsData.iteritems():
        strout = '{name} 0.0 0.0 0.0 {lat} {lon} 0.0 2010.0 0.0 0.0 0.0 0.0 0.0 0.0\n'.format(name=stn.name.lower(), lat=stn.lat, lon=stn.lon)
        fid.write(strout)
    fid.close()

####Create station-by-station file for stations
    if not os.path.exists(gpsdir):
        os.mkdir(gpsdir)

    for key,stn in gpsData.iteritems():
        stnname = stn.name.lower()
        fname = os.path.join(gpsdir, '{0}CleanFlt.neu'.format(stnname))
        stn.dump_sopac_file(fname)

    return


def cmdLineParse():
    '''
    Command line parser.
    '''
    '''
    parser = argparse.ArgumentParser(description='Setup auxiliary data for GIAnT processing')
    parser.add_argument('--nolw', action='store_true', default=False,
            help='If no land water mask is desired.', dest='nolw')
    parser.add_argument('--nogps', action='store_true', default=False,
            help='If no GPS data is desired.', dest='nogps')
    parser.add_argument('--dir', action='store', default='./insar',
            type=str, dest='insarDir',
            help = 'Directory where all the interfeograms have been staged.')
    parser.add_argument('--mask', action='store', default='mask.flt',
            type=str, dest='maskName',
            help='Name of output mask file.')
    parser.add_argument('--lwmask', action='store', default='landmask.flt',
            type=str, dest='lwmaskName',
            help='Name of output land mask file.')
    parser.add_argument('--ref', action='store', default='ref.in',
            type=str, dest='refName',
            help='Name of the reference pixel location file.')
    parser.add_argument('--gps', action='store', default='GPS',
            type=str, dest='gpsDir',
            help='Name of directory to store GPS data.')
    parser.add_argument('--cthresh', action='store', default='0.2',
            type=float, dest='cthresh',
            help='Coherence threshold for mask.')
    parser.add_argument('--gpswin', action='store', default=5,
            type=int, dest='gpswin',
            help = 'GPS window size to match coherent pixels.')
    parser.add_argument('--gpsout', action='store', default='gps.cpck',
            type=str, dest='gpsout',
            help = 'Output GPS file in json format.')

    return parser.parse_args()
    '''
    return json.load(open(sys.argv[1]))

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

    #####Land Water mask
    if not inps['nolw']:
        getLandWaterMask(metaData['snwe'], (metaData['length'], metaData['width']), os.path.join(currDir, inps['lwmaskName']))

        allMask = np.fromfile(os.path.join(currDir, inps['lwmaskName']), dtype=np.float32).reshape((metaData['length'],metaData['width']))
        allMask[allMask == 0.] = np.nan
        allMask[allMask == 1.] = 0.
    else:
        print 'Skipping Land Water mask. Not requested.'
        allMask = np.zeros((metaData['length'], metaData['width']))

    ####Get coherence files and build coherence mask
    for ii in intList:
        cohname = os.path.join(ii, 'phsig.cor.geo')
        try:
            incoh = np.fromfile(cohname, dtype=np.float32).reshape((metaData['length'], metaData['width']))
        except:
            print 'Error reading coherence file: ', cohname
            raise Exception('Coherence file size does not match')
       
        allMask[incoh > inps['cthresh_ax']] += 1.0

    del incoh
    #####Normalize sum of coherence between 0 and 1
    allMask = allMask/(1.0*len(intList))

    ####Atleast half the interferograms need to be coherent
    allMask[allMask < 0.5] = np.nan
    allMask[allMask >= 0.5] = 1.0

    ######Write coherence mask as well
    allMask.astype(np.float32).tofile(os.path.join(currDir, inps['maskName']))

    ######GPS data handling
    sarList = veloLib.getDatesFromIntList(intList)
    if inps['nogps']:
        print 'GPS data not requested.'
        print 'Choosing point that is coherent in most interferograms.'
    
        try:
            refPos = np.unravel_index(np.nanargmax(allMask))
        except:
            print 'Could not find most coherent pixel.'
            sys.exit(errorCodes['Not enough coherence'])

    else:
        ####First get GPS data for the master SAR acquisition
        try:
            masterGPS = GPSlib.getGPSinBox(sarList[0], metaData['snwe'])
        except:
            print 'Unable to get GPS data for master date'
            sys.exit(errorCodes['GPS Data Error'])

        gpsData = OrderedDict()

        for site, gps in masterGPS.iteritems():
            ii = np.int(np.round((gps.wgsLat - metaData['snwe'][1])/metaData['deltaLat']))
            jj = np.int(np.round((gps.wgsLon - metaData['snwe'][2])/metaData['deltaLon']))

            if (ii > inps['gpswin']) and (ii < (metaData['length'] - inps['gpswin'])):
                if (jj > inps['gpswin']) and (jj < (metaData['width'] - inps['gpswin'])):
                    msk = np.nansum(1*np.isfinite(allMask[ii-inps['gpswin']:ii+inps['gpswin'], jj-inps['gpswin']:jj+inps['gpswin']]))
                    if msk > 0:
                        gps.setupLocalCoordinates()
                        gpsData[site] = GPSlib.GPSstn(gps.site, gps.wgsLat, gps.wgsLon, ii, jj)
                        gpsData[site].addObservation(sarList[0], np.zeros(3), gps.refError)


        #####Check if enough GPS stations are available
        if len(gpsData) < 5:
            print 'Less than 5 GPS stations over the frame'
            print 'Try manual processing or without GPS'
            sys.exit(errorCodes['Not enough GPS points'])

        ####Check if there is sufficient overlap between InSAR and GPS
        if np.sum(np.isfinite(allMask)) == 0:
            print 'Not enough coherence around GPS stations.'
            sys.exit(errorCodes['Not enough GPS points'])

       
        ####Loop over the SAR dates to create ENU observation array
        for date in sarList[1:]:
            try:
                slaveGPS = GPSlib.getGPSinBox(date, metaData['snwe'])
            except:
                print 'Unable to get GPS data for date: '+date
                sys.exit(errorCodes['GPS Data Error'])

            for site,gps  in gpsData.iteritems():
                if site not in slaveGPS.keys():
                    del masterGPS[site]
                else:
                    enu,err = masterGPS[site].toENU(slaveGPS[site])
                    gps.addObservation(date, 1000*enu, 1000*err)

            if len(gpsData) < 5:
                print 'Number of GPS points available < 5'
                sys.exit(errorCodes['Not enough GPS points'])

        print 'Number of viable GPS stations: ', len(gpsData)

        ####Pick the GPS station with least displacement as reference 
        displacement = []

        for site, gps in gpsData.iteritems():
            print site, np.linalg.norm(gps.getENU(sarList[-1]))
            displacement.append(np.linalg.norm(gps.getENU(sarList[-1])))

        '''
        minDispArg = np.nanargmin(np.array(displacement))

        if np.isnan(minDispArg):
            print 'Error: Min GPS displacmeent is NaN'
            sys.exit(errorCodes['GPS Data Error'])
        '''
        msk = np.reshape(np.fromfile(inps['qamaskName']),(metaData['length'],metaData['width']))
        isrt = np.argsort(displacement)
        for i in isrt:
            siteKey = gpsData.keys()[i]
            refPos = gpsData[siteKey].getPosition()
            if(refPos[0] <= metaData['length'] and refPos[1] <= metaData['width'] and  msk[refPos[0],refPos[1]] > 0):
                break
            
        print siteKey, refPos

        #####Write the reference location to file
        fid = open(inps['refName'], 'w')
        fid.write("{0}\n".format(refPos[0]))
        fid.write("{0}\n".format(refPos[1]))
        fid.close()

        ####Write GPS data to file
        #fid = open(inps.gpsout, 'wb')
        #cPickle.dump(gpsData, fid)
        #fid.close()

###Dump GPS data
        createGIAnTGPSrepo(gpsData)    
        

