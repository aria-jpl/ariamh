#!/usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#Author: Piyush Agram
#Copyright 2014, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#Any commercial use must be negotiated with the Office of Technology Transfer at the California Institute of Technology.
#
#This software may be subject to U.S. export control laws. By accepting this software, the user agrees to comply with all applicable U.S.
#export laws and regulations. User has the responsibility to obtain export licenses, or other export authority as may be required before 
#exporting such information to foreign countries or providing access to foreign persons.
#
#                        NASA Jet Propulsion Laboratory
#                      California Institute of Technology
#                        (C) 2010  All Rights Reserved
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import isce
from xml.etree.ElementTree import ElementTree
import datetime
import isceobj
from BurstSLC import BurstSLC
from isceobj.Util import Poly1D, Poly2D
from isceobj.Planet.Planet import Planet
from isceobj.Orbit.Orbit import StateVector, Orbit
from isceobj.Orbit.OrbitExtender import OrbitExtender
from isceobj.Planet.AstronomicalHandbook import Const
from iscesys.Component.Component import Component
from iscesys.DateTimeUtil.DateTimeUtil import DateTimeUtil as DTUtil
import os
import numpy as np
import pickle
import re

sep = "\n"
tab = "    "
lookMap = { 'RIGHT' : -1,
            'LEFT' : 1}

class Sentinel1_TOPS(Component):
    """
        A Class representing Sentinel-1 data
    """
    def __init__(self):
        Component.__init__(self)        
        self.xml = None
        self.tiff = None
        self.orbitFile = None
        self.outdir=None
        self.numberBursts = None
        self.bursts = []
         
        
        self._xml_root=None
        self.descriptionOfVariables = {}
        self.dictionaryOfVariables = {'XML': ['self.xml','str','mandatory'],
                                      'TIFF': ['self.tiff','str','mandatory'],
                                      'PREFIX': ['self.prefix','str','optional']}
        
                                               
    def parse(self):
        try:
            fp = open(self.xml,'r')
        except IOError as strerr:
            print("IOError: %s" % strerr)
            return
        self._xml_root = ElementTree(file=fp).getroot()
        self.numberBursts = self.getNumberOfBursts()

        for kk in range(self.numberBursts):
            slc = BurstSLC()
            slc.configure()
            self.bursts.append(slc)

        self.populateCommonMetadata()

        self.populateBurstSpecificMetadata()

        ####Read in the orbits
        if self.orbitFile:
            orb = self.extractPreciseOrbit()
        else:
            orb = self.extractOrbit()

        for burst in self.bursts:
            burst.orbit.setOrbitSource('Header')

            for sv in orb:
                burst.orbit.addStateVector(sv)



        fp.close()

    def getxmlattr(self, path, key):
        try:
            res = self._xml_root.find(path).attrib[key]
        except:
            raise Exception('Cannot find attribute %s at %s'%(key, path))

        return res

    def getxmlvalue(self, path):
        try:
            res = self._xml_root.find(path).text
        except:
            raise Exception('Tag= %s not found'%(path))

        if res is None:
            raise Exception('Tag = %s not found'%(path))

        return res

    def getxmlelement(self, path):
        try:
            res = self._xml_root.find(path)
        except:
            raise Exception('Cannot find path %s'%(path))

        if res is None:
            raise Exception('Cannot find path %s'%(path))

        return res

    def convertToDateTime(self, string):
        dt = datetime.datetime.strptime(string,"%Y-%m-%dT%H:%M:%S.%f")
        return dt

    def getNumberOfBursts(self):
        return int(self.getxmlattr('swathTiming/burstList', 'count'))
        
    
    def populateCommonMetadata(self):
        """
            Create metadata objects from the metadata files
        """
        ####Set each parameter one - by - one
        mission = self.getxmlvalue('adsHeader/missionId')
        swath = self.getxmlvalue('adsHeader/swath')
        polarization = self.getxmlvalue('adsHeader/polarisation')
        orbitnumber = int(self.getxmlvalue('adsHeader/absoluteOrbitNumber'))
        frequency = float(self.getxmlvalue('generalAnnotation/productInformation/radarFrequency'))
        passDirection = self.getxmlvalue('generalAnnotation/productInformation/pass')

        rangeSampleRate = float(self.getxmlvalue('generalAnnotation/productInformation/rangeSamplingRate'))
#        rangePixelSize = float(self.getxmlvalue('imageAnnotation/imageInformation/rangePixelSpacing'))
        rangePixelSize = Const.c/(2.0*rangeSampleRate)
        azimuthPixelSize = float(self.getxmlvalue('imageAnnotation/imageInformation/azimuthPixelSpacing'))
        azimuthTimeInterval = float(self.getxmlvalue('imageAnnotation/imageInformation/azimuthTimeInterval'))

        lines = int(self.getxmlvalue('swathTiming/linesPerBurst'))
        samples = int(self.getxmlvalue('swathTiming/samplesPerBurst'))

        startingRange = float(self.getxmlvalue('imageAnnotation/imageInformation/slantRangeTime'))*Const.c/2.0
        incidenceAngle = float(self.getxmlvalue('imageAnnotation/imageInformation/incidenceAngleMidSwath'))

        steeringRate = np.radians(float( self.getxmlvalue('generalAnnotation/productInformation/azimuthSteeringRate')))

        prf = float(self.getxmlvalue('generalAnnotation/downlinkInformationList/downlinkInformation/prf'))
        terrainHeight = float(self.getxmlvalue('generalAnnotation/terrainHeightList/terrainHeight/value'))
        ####Sentinel is always right looking
        lookSide = -1

        # relative orbit (track); per https://scihub.copernicus.eu/news/News00014
        if mission == "S1A":
            trackNumber = (orbitnumber-73)%175+1
        elif mission == "S1B":
            trackNumber = (orbitnumber-27)%175+1
        else:
            raise NotImplementedError("Error calculating track number for unknown mission: %s" % mission)
           

        for burst in self.bursts:
            burst.numberOfSamples = samples
            burst.numberOfLines = lines
            burst.startingRange = startingRange
            burst.trackNumber = trackNumber
            burst.orbitNumber = orbitnumber 
            burst.frameNumber = 1 
            burst.polarization = polarization
            burst.swath = swath
            burst.passDirection = passDirection
            burst.radarWavelength = Const.c / frequency
            burst.rangePixelSize = rangePixelSize
            burst.azimuthTimeInterval = azimuthTimeInterval
            burst.azimuthSteeringRate = steeringRate
            burst.prf = prf
            burst.terrainHeight = terrainHeight




    def populateBurstSpecificMetadata(self):
        '''
        Extract burst specific metadata from the xml file.
        '''
        
        burstList = self.getxmlelement('swathTiming/burstList')
        for index, burst in enumerate(burstList.getchildren()):
            bb = self.bursts[index]
            bb.sensingStart = self.convertToDateTime(burst.find('azimuthTime').text)
            deltaT = datetime.timedelta(seconds=(bb.numberOfLines - 1)*bb.azimuthTimeInterval)
            bb.sensingStop = bb.sensingStart + deltaT

            bb.sensingMid = bb.sensingStart + datetime.timedelta(seconds = 0.5 * deltaT.total_seconds()) 

            bb.startUTC = self.convertToDateTime(burst.find('sensingTime').text)
            deltaT = datetime.timedelta(seconds=(bb.numberOfLines-1)/bb.prf)
            bb.stopUTC = bb.startUTC + deltaT
            bb.midUTC  = bb.startUTC + datetime.timedelta(seconds = 0.5*deltaT.total_seconds())

            firstValidSample = [int(val) for val in burst.find('firstValidSample').text.split()]

            first=False
            last=False
            count=0
            for ii, val in enumerate(firstValidSample):
                if (val >= 0) and (not first):
                    first = True
                    bb.firstValidLine = ii

                if (val < 0) and (first) and (not last):
                    last = True
                    bb.numValidLines = ii - bb.firstValidLine
            

        ####Read in fm rates separately
        fmrateList = self.getxmlelement('generalAnnotation/azimuthFmRateList')
        fmRates = []
        for index, burst in enumerate(fmrateList.getchildren()):
            r0 = 0.5 * Const.c * float(burst.find('t0').text)
            try:
                c0 = float(burst.find('c0').text)
                c1 = float(burst.find('c1').text)
                c2 = float(burst.find('c2').text)
                coeffs = [c0,c1,c2]
            except AttributeError:
                coeffs = [float(val) for val in burst.find('azimuthFmRatePolynomial').text.split()]

            refTime = self.convertToDateTime(burst.find('azimuthTime').text)
            poly = Poly1D.Poly1D()
            poly.initPoly(order=len(coeffs)-1)
            poly.setMean(r0)
            poly.setNorm(0.5*Const.c)
            poly.setCoeffs(coeffs)

            fmRates.append((refTime, poly))

        for index, burst in enumerate(self.bursts):

            dd = [ np.abs((burst.sensingMid - val[0]).total_seconds()) for val in fmRates]

            arg = np.argmin(dd)
            burst.azimuthFMRate = fmRates[arg][1]

            print('FM rate matching: Burst %d to Poly %d'%(index, arg))



        dcList = self.getxmlelement('dopplerCentroid/dcEstimateList')
        dops = [ ]
        for index, burst in enumerate(dcList.getchildren()):

            r0 = 0.5 * Const.c* float(burst.find('t0').text)
            refTime = self.convertToDateTime(burst.find('azimuthTime').text)
            coeffs = [float(val) for val in burst.find('dataDcPolynomial').text.split()]
            poly = Poly1D.Poly1D()
            poly.initPoly(order=len(coeffs)-1)
            poly.setMean(r0)
            poly.setNorm(0.5*Const.c)
            poly.setCoeffs(coeffs)

            dops.append((refTime, poly))

        for index, burst in enumerate(self.bursts):

            dd = [np.abs((burst.sensingMid - val[0]).total_seconds()) for val in dops]
            
            arg = np.argmin(dd)
            burst.doppler = dops[arg][1]

            print('Doppler matching: Burst %d to Poly %d'%(index, arg))
        
    def extractOrbit(self):
        '''
        Extract orbit information from xml node.
        '''
        node = self._xml_root.find('generalAnnotation/orbitList')

        frameOrbit = Orbit()
        frameOrbit.configure()

        for child in node.getchildren():
            timestamp = self.convertToDateTime(child.find('time').text)
            pos = []
            vel = []
            posnode = child.find('position')
            velnode = child.find('velocity')
            for tag in ['x','y','z']:
                pos.append(float(posnode.find(tag).text))

            for tag in ['x','y','z']:
                vel.append(float(velnode.find(tag).text))

            vec = StateVector()
            vec.setTime(timestamp)
            vec.setPosition(pos)
            vec.setVelocity(vel)
            frameOrbit.addStateVector(vec)
            print(vec)


        orbExt = OrbitExtender(planet=Planet(pname='Earth'))
        orbExt.configure()
        newOrb = orbExt.extendOrbit(frameOrbit)


        return newOrb
            
    def extractPreciseOrbit(self):
        '''
        Extract precise orbit from given Orbit file.
        '''
        try:
            fp = open(self.orbitFile,'r')
        except IOError as strerr:
            print("IOError: %s" % strerr)
            return

        _xml_root = ElementTree(file=fp).getroot()
       
        node = _xml_root.find('Data_Block/List_of_OSVs')

        orb = Orbit()
        orb.configure()

        margin = datetime.timedelta(seconds=40.0)
        tstart = self.bursts[0].sensingStart - margin
        tend = self.bursts[-1].sensingStop + margin

        for child in node.getchildren():
            timestamp = self.convertToDateTime(child.find('UTC').text[4:])

            if (timestamp >= tstart) and (timestamp < tend):

                pos = [] 
                vel = []

                for tag in ['VX','VY','VZ']:
                    vel.append(float(child.find(tag).text))

                for tag in ['X','Y','Z']:
                    pos.append(float(child.find(tag).text))

                vec = StateVector()
                vec.setTime(timestamp)
                vec.setPosition(pos)
                vec.setVelocity(vel)
                print(vec)
                orb.addStateVector(vec)

        fp.close()

        return orb


    def extractImage(self, action=True):
        """
           Use gdal python bindings to extract image
        """
        try:
            from osgeo import gdal
        except ImportError:
            raise Exception('GDAL python bindings not found. Need this for RSAT2/ TandemX / Sentinel1.')

        self.parse()

        src = gdal.Open(self.tiff.strip(), gdal.GA_ReadOnly)
        band = src.GetRasterBand(1)

        print('Total Width  = %d'%(src.RasterXSize))
        print('Total Length = %d'%(src.RasterYSize))

        lineOffset = 0

        if os.path.isdir(self.outdir):
            print('Output directory {0} already exists.'.format(self.outdir))
        else:
            print('Creating directory {0} '.format(self.outdir))
            os.makedirs(self.outdir)

        for index, burst in enumerate(self.bursts):
            outfile = os.path.join(self.outdir, 'burst_%02d'%(index+1) + '.slc')

            if action:
                ###Write original SLC to file
                fid = open(outfile, 'wb')
                data = band.ReadAsArray(0, lineOffset, burst.numberOfSamples, burst.numberOfLines)
                data.tofile(fid)
                fid.close()

            ####Render ISCE XML
            slcImage = isceobj.createSlcImage()
            slcImage.setByteOrder('l')
            slcImage.setFilename(outfile)
            slcImage.setAccessMode('read')
            slcImage.setWidth(burst.numberOfSamples)
            slcImage.setLength(burst.numberOfLines)
            slcImage.setXmin(0)
            slcImage.setXmax(burst.numberOfSamples)
            slcImage.renderHdr()
            burst.image = slcImage 

            lineOffset += burst.numberOfLines

        band = None
        src = None

    def computeRamp(self, burst, offset=0.0, position=None):
        '''
        Returns the ramp function as a numpy array.
        '''
        Vs = np.linalg.norm(burst.orbit.interpolateOrbit(burst.sensingMid, method='hermite').getVelocity())
        Ks =   2 * Vs * burst.azimuthSteeringRate / burst.radarWavelength 
        cJ = np.complex64(1.0j)

        if position is None:
            rng = np.arange(burst.numberOfSamples) * burst.rangePixelSize + burst.startingRange

## Seems to work best for basebanding data
            eta =( np.arange(0, burst.numberOfLines) - (burst.numberOfLines//2)) * burst.azimuthTimeInterval +  offset * burst.azimuthTimeInterval

            f_etac = burst.doppler(rng)
            Ka     = burst.azimuthFMRate(rng)

            eta_ref = (burst.doppler(burst.startingRange) / burst.azimuthFMRate(burst.startingRange) ) - (f_etac / Ka)

#            eta_ref *= 0.0
            Kt = Ks / (1.0 - Ks/Ka)


            ramp = np.exp(-cJ * np.pi * Kt[None,:] * ((eta[:,None] - eta_ref[None,:])**2))

        else:
            ####y and x need to be zero index
            y,x = position

            eta = (y - (burst.numberOfLines//2)) * burst.azimuthTimeInterval + offset * burst.azimuthTimeInterval
            rng = burst.startingRange + x * burst.rangePixelSize 
            f_etac = burst.doppler(rng)
            Ka  = burst.azimuthFMRate(rng)

            eta_ref = (burst.doppler(burst.startingRange) / burst.azimuthFMRate(burst.startingRange)) - (f_etac / Ka)
#            eta_ref *= 0.0
            Kt = Ks / (1.0 - Ks/Ka)

            ramp = np.exp(-cJ * np.pi * Kt * ((eta - eta_ref)**2))

        return ramp




    def derampImage(self, offset=0.0, action=True):
        '''
        Deramp the bursts.
        '''

        t0 = self.bursts[0].sensingStart

        lineOffset = 0
        for index, burst in enumerate(self.bursts):
            infile = burst.image.filename
            derampfile = os.path.join(self.outdir, 'deramp_%02d'%(index+1) + '.slc')


            if action:
                data = np.fromfile(infile, dtype=np.complex64).reshape((burst.numberOfLines, burst.numberOfSamples))


                ramp = self.computeRamp(burst, offset=offset)

                #####Write Deramped SLC to file
                data *= ramp


                print('Burst Number: %d'%(index+1))
                print('Number of Lines: %d'%(burst.numberOfLines))
                print('Global Offset: %d' %(np.round(-(t0 - burst.sensingStart).total_seconds() / burst.azimuthTimeInterval)))
                if index > 0:
                    boff = (np.round(-(self.bursts[index-1].sensingStart - burst.sensingStart).total_seconds() / burst.azimuthTimeInterval))
                    lineOffset += boff
                    print('Burst OFfset: %d'%lineOffset)

                fid = open(derampfile, 'wb')
                data.tofile(fid)
                fid.close()


            ####Render ISCE XML
            slcImage = isceobj.createSlcImage()
            slcImage.setByteOrder('l')
            slcImage.setFilename(derampfile)
            slcImage.setAccessMode('read')
            slcImage.setWidth(burst.numberOfSamples)
            slcImage.setLength(burst.numberOfLines)
            slcImage.setXmin(0)
            slcImage.setXmax(burst.numberOfSamples)
            slcImage.renderHdr()
            burst.derampimage = slcImage 

        band = None
        src = None



    def mergeDeramped(self):
        '''
        Merge deramped SLCs into single file.
        '''

        nBursts = len(self.bursts)
       
        t0 = self.bursts[0].sensingStart
        dt = self.bursts[0].azimuthTimeInterval
        width = self.bursts[0].numberOfSamples

        tstart = t0 + datetime.timedelta(seconds = (dt*self.bursts[0].firstValidLine))
        tend   = self.bursts[-1].sensingStart + datetime.timedelta(seconds=((self.bursts[-1].firstValidLine + self.bursts[-1].numValidLines-1) * dt))

        nLines = int( np.round((tend - tstart).total_seconds() / dt)) + 1
        print('Expected nLines: ', nLines)

        azMasterOff = []
        for index, burst in enumerate(self.bursts):
            soff = burst.sensingStart + datetime.timedelta(seconds = (burst.firstValidLine*dt)) 
            start = int(np.round((soff - tstart).total_seconds() / dt))
            end = start + burst.numValidLines

            azMasterOff.append([start,end])

            print('Burst: ', index, [start,end])

        outfile = os.path.join(self.outdir, 'merged.slc')

        fid = open(outfile, 'wb')

        for index in range(nBursts):
            curBurst = self.bursts[index]
            curLimit = azMasterOff[index]
            curData =  np.fromfile(curBurst.derampimage.filename, dtype=np.complex64).reshape((-1, width))[curBurst.firstValidLine: curBurst.firstValidLine + curBurst.numValidLines,:]

            #####If middle burst
            if index > 0:
                topBurst = self.bursts[index-1]
                topLimit = azMasterOff[index-1]
                topData =  np.fromfile(topBurst.derampimage.filename, dtype=np.complex64).reshape((-1, width))[topBurst.firstValidLine: topBurst.firstValidLine + topBurst.numValidLines,:]

                olap = topLimit[1] - curLimit[0]

                if olap <= 0:
                    raise Exception('No Burst Overlap')


                im1 = topData[-olap:,:]
                im2 = curData[:olap,:]

                print('Offsets: ', index, getOffset(im1,im2))

                data = 0.5*(im1 + im2)
                data.tofile(fid)

                tlim = olap
            else:
                tlim = 0

            
            if index != (nBursts-1):
                botBurst = self.bursts[index+1]
                botLimit = azMasterOff[index+1]
            
                olap = curLimit[1] - botLimit[0]

                if olap < 0:
                    raise Exception('No Burst Overlap')

                blim = botLimit[0] - curLimit[0]
            else:
                blim = None

            

            data = curData[tlim:blim,:]
            data.tofile(fid)

        fid.close()


        img = isceobj.createSlcImage()
        img.setWidth(width)
        img.setAccessMode('READ')
        img.setFilename(outfile)

        img.createImage()
        img.renderHdr()
        img.finalizeImage()



def getOffset(mas, slv):
    res = []
    for xpos in [4096, 8192, 16384]:
        m1 = np.fft.fft2(np.abs(mas[:,xpos:xpos+1024]))
        m2 = np.fft.fft2(np.abs(slv[:,xpos:xpos+1024]))
        g = np.abs(np.fft.ifft2(m1 * np.conj(m2)))
        res.append(np.unravel_index(g.argmax(), g.shape))

    return res

def createParser():
    import argparse

    parser = argparse.ArgumentParser( description = 'Sentinel parser' )
    parser.add_argument('-x','--xml', dest='xml', type=str,
            default=None, help='Annotation XML file.')

    parser.add_argument('-t', '--tiff', dest='tiff', type=str,
            default=None, help='Measurement Geotiff file.')

    parser.add_argument('-d', '--dirname', dest='dirname', type=str,
            default=None, help='SAFE format directory')

    parser.add_argument('-s', '--swathnum', dest='swathnum', type=int,
            default=None, help='Swath number for analysis')

    parser.add_argument('-o', '--outdir', dest='outdir', type=str,
            required=True, help='Output SLC prefix.')

    parser.add_argument('-m', '--misregistration', dest='misregistration', type=float,
            default=0.0, help='Misregistration in azimuth pixels ')

    parser.add_argument('-p', '--orbit', dest='orbit', type=str,
            default=None, help='Precise orbit file')

    return parser

def cmdLineParse(iargs=None):
    '''
    Command Line Parser.
    '''
    parser = createParser()
    inps = parser.parse_args(args=iargs)

    if (inps.dirname is None) and (inps.swathnum is None):
        #Tiff and XML directly provided
        if inps.tiff is None:
            raise Exception('TIFF file not provided')

        if inps.xml is None:
            raise Exception('XML file not provided')

    else:
        if inps.swathnum is None:
            raise Exception('Desired swath number is not provided')

        if inps.dirname is None:
            raise Exception('SAFE directory is not provided')

        
        ####First find annotation file
        swathid = 's1.-iw%d'%(inps.swathnum)
        swathid_re = re.compile(swathid)
        polid = 'vv'
        for root, dirs, files in os.walk( os.path.join(inps.dirname, 'annotation')):
            for fil in files:
                match = None
                if swathid_re.search(fil) and (polid in fil):
                    match = fil
                    print('MATCH = ', match)
                    break

            if match is not None:
                break

        inps.xml = os.path.join(inps.dirname, 'annotation', match)
        print('XML file: ', inps.xml)

        ####Find TIFF file
        for root, dirs, files in os.walk( os.path.join(inps.dirname, 'measurement')):
            for fil in files:
                match = None
                if swathid_re.search(fil) and (polid in fil):
                    match = fil
                    break

            if match is not None:
                break

        inps.tiff = os.path.join(inps.dirname, 'measurement', match)
        print('TIFF file: ', inps.tiff)
        

    return inps

def main(iargs=None):
    inps = cmdLineParse(iargs)

    obj =Sentinel1_TOPS()
    obj.configure()
    obj.xml = inps.xml
    obj.tiff = inps.tiff
    obj.outdir = inps.outdir
    obj.orbitFile = inps.orbit

    obj.extractImage()

#    obj.derampImage(offset=inps.misregistration)

    with open(os.path.join(inps.outdir, 'data.pck'), 'wb') as f:
        pickle.dump(obj, f)

if __name__ == '__main__':

    main()

