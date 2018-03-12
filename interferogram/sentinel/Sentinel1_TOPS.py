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
import glob
import numpy as np
import shelve
import re

sep = "\n"
tab = "    "
lookMap = { 'RIGHT' : -1,
            'LEFT' : 1}

class Sentinel1_TOPS(Component):
    """
        A Class representing RadarSAT 2 data
    """
    def __init__(self):
        Component.__init__(self)        
        self.xml = None
        self.tiff = None
        self.orbitFile = None
        self.auxFile = None
        self.orbitDir = None
        self.auxDir = None
        self.manifest = None
        self.IPFversion = None
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
            slc.burstNumber = kk+1
            self.bursts.append(slc)

        self.populateCommonMetadata()

        self.populateBurstSpecificMetadata()

        ####Tru and locate an orbit file
        if self.orbitFile is None:
            if self.orbitDir is not None:
                self.orbitFile = self.findOrbitFile()

        
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

        self.populateIPFVersion()

        if self.IPFversion == '002.36':
            '''Range dependent correction needed.'''
            if self.auxFile is None:
                self.auxFile = self.findAuxFile()

            if self.auxFile is None:
                print('******************************')
                print('Warning:  Strongly recommend using auxiliary information')
                print('          when using products generated with IPF 002.36')
                print('******************************')
        for burst in self.bursts:
            burst.IPFversion = self.IPFversion
            burst.auxFile = self.auxFile

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
        
        slantRangeTime = float(self.getxmlvalue('imageAnnotation/imageInformation/slantRangeTime'))
        startingRange = float(self.getxmlvalue('imageAnnotation/imageInformation/slantRangeTime'))*Const.c/2.0
        incidenceAngle = float(self.getxmlvalue('imageAnnotation/imageInformation/incidenceAngleMidSwath'))
        steeringRate = np.radians(float( self.getxmlvalue('generalAnnotation/productInformation/azimuthSteeringRate')))

        slantRangeTimeSub = self.getxmlvalue('antennaPattern/antennaPatternList/antennaPattern/slantRangeTime')        
        elevationAngle = self.getxmlvalue('antennaPattern/antennaPatternList/antennaPattern/elevationAngle')

        prf = float(self.getxmlvalue('generalAnnotation/downlinkInformationList/downlinkInformation/prf'))
        terrainHeight = float(self.getxmlvalue('generalAnnotation/terrainHeightList/terrainHeight/value'))
        ####Sentinel is always right looking
        lookSide = -1

        ###Read ascending node for phase calibration
        ascTime = self.convertToDateTime(self.getxmlvalue('imageAnnotation/imageInformation/ascendingNodeTime'))
        
        for burst in self.bursts:
            burst.numberOfSamples = samples
            burst.numberOfLines = lines
            burst.startingRange = startingRange
            burst.trackNumber = (orbitnumber-73)%175+1 # per https://scihub.copernicus.eu/news/News00014
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
            burst.ascendingNodeTime = ascTime
            burst.rangeSampleRate = rangeSampleRate
            burst.slantRangeTime = slantRangeTime
            burst.slantRangeTimeSub = [float(val) for val in slantRangeTimeSub.split()]
            burst.elevationAngle = [float(val) for val in elevationAngle.split()]

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
            lastValidSample = [int(val) for val in burst.find('lastValidSample').text.split()]

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
           
            lastLine = bb.firstValidLine + bb.numValidLines - 1

            bb.firstValidSample = max(firstValidSample[bb.firstValidLine], firstValidSample[lastLine])
            lastSample = min(lastValidSample[bb.firstValidLine], lastValidSample[lastLine])

            bb.numValidSamples = lastSample - bb.firstValidSample

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

#            print('FM rate matching: Burst %d to Poly %d'%(index, arg))



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

#            print('Doppler matching: Burst %d to Poly %d'%(index, arg))

    def populateIPFVersion(self):
        '''
        Get IPF version from the manifest file.
        '''

        if self.manifest is None:
            return

        nsp = "{http://www.esa.int/safe/sentinel-1.0}"

        try:
            fp = open(self.manifest, 'r')
            root = ElementTree(file=fp).getroot()
            fp.close()
        
            elem = root.find('.//metadataObject[@ID="processing"]')
            rdict = elem.find('.//xmlData/' + nsp + 'processing/' + nsp + 'facility/' + nsp + 'software').attrib

            self.IPFversion = rdict['version']
            print('Setting IPF version to : ', self.IPFversion) 

        except:
            print('Could not read version number successfully from manifest file: ', self.manifest)
            pass
        return

    def findOrbitFile(self):
        '''
        Find correct orbit file in the orbit directory.
        '''

        datefmt = "%Y%m%dT%H%M%S"
        types = ['POEORB', 'RESORB']
        match = []
        nbursts = len(self.bursts)
        timeStamp = self.bursts[nbursts//2].sensingMid

        for orbType in types:
            files = glob.glob( os.path.join(self.orbitDir, 'S1?_OPER_AUX_' + orbType + '_OPOD*'))
            
            ###List all orbit files
            for result in files:
                fields = result.split('_')
                taft = datetime.datetime.strptime(fields[-1][0:15], datefmt)
                tbef = datetime.datetime.strptime(fields[-2][1:16], datefmt)
                
                #####Get all files that span the acquisition
                if (tbef <= timeStamp) and (taft >= timeStamp):
                    tmid = tbef + 0.5 * (taft - tbef)
                    match.append((result, abs((timeStamp-tmid).total_seconds())))

                #####Return the file with the image is aligned best to the middle of the file
                if len(match) != 0:
#                    print('Matches: ', match)
                    bestmatch = min(match, key = lambda x: x[1])
#                    print('Best match: ', bestmatch)
                    return bestmatch[0]

       
            if len(match) == 0:
                 raise Exception('No suitable orbit file found. If you want to process anyway - unset the orbitdir parameter')

    def findAuxFile(self):
        '''
        Find appropriate auxiliary information file.
        '''

        datefmt = "%Y%m%dT%H%M%S"
        
        match = []
        nbursts = len(self.bursts)
        timeStamp = self.bursts[nbursts//2].sensingMid

        files = glob.glob(os.path.join(self.auxDir, 'S1?_AUX_CAL_*'))
        
        ###List all orbit files
        for result in files:
            fields = result.split('_')
            taft = datetime.datetime.strptime(fields[-1][1:16], datefmt)
            tbef = datetime.datetime.strptime(fields[-2][1:16], datefmt)
                
            #####Get all files that span the acquisition
            if (tbef <= timeStamp) and (taft >= timeStamp):
                tmid = tbef + 0.5 * (taft - tbef)
                match.append((result, abs((timeStamp-tmid).total_seconds())))

        #####Return the file with the image is aligned best to the middle of the file
        if len(match) != 0:
#            print('Matches: ', match)
            bestmatch = min(match, key = lambda x: x[1])
#            print('Best match: ', bestmatch)
            return glob.glob(os.path.join(bestmatch[0], 'data/s1?-aux-cal.xml'))[0]

       
            if len(match) == 0:
                print('******************************************')
                print('Warning: Aux file requested but no suitable auxiliary file found.')
                print('******************************************')

            return None

    def extractOrbit(self):
        '''
        Extract orbit information from xml node.
        '''
        node = self._xml_root.find('generalAnnotation/orbitList')

        print('Extracting orbit from annotation XML file')
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
#            print(vec)


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

        print('Extracting orbit from Orbit File: ', self.orbitFile)
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
#                print(vec)
                orb.addStateVector(vec)

        fp.close()

        return orb


    def extractImage(self, nameOffset=0, action=True, parse=True,
            width=None, length = None):
        """
           Use gdal python bindings to extract image
        """
        try:
            from osgeo import gdal
        except ImportError:
            raise Exception('GDAL python bindings not found. Need this for RSAT2/ TandemX / Sentinel1.')

        if parse:
            self.parse()

   
        ###If not specified, for single slice, use width and length from first burst
        if width is None:
            width = self.bursts[0].numberOfSamples

        if length is None:
            length = self.bursts[0].numberOfLines

        src = gdal.Open(self.tiff.strip(), gdal.GA_ReadOnly)
        band = src.GetRasterBand(1)

        print('Total Width  = %d'%(src.RasterXSize))
        print('Total Length = %d'%(src.RasterYSize))

        if os.path.isdir(self.outdir):
            print('Output directory {0} already exists.'.format(self.outdir))
        else:
            print('Creating directory {0} '.format(self.outdir))
            os.makedirs(self.outdir)

        for index, burst in enumerate(self.bursts):
            outfile = os.path.join(self.outdir, 'burst_%02d'%(nameOffset+index+1) + '.slc')
            originalWidth = burst.numberOfSamples
            originalLength = burst.numberOfLines
            
            if action:
                ###Write original SLC to file
                fid = open(outfile, 'wb')

                ####Use burstnumber to look into tiff file
                lineOffset = (burst.burstNumber-1) * burst.numberOfLines

                ###Read whole burst for debugging. Only valid part is used.
                data = band.ReadAsArray(0, lineOffset, burst.numberOfSamples, burst.numberOfLines)

                ###Create output array and copy in valid part only
                ###Easier then appending lines and columns.
                outdata = np.zeros((length,width), dtype=np.complex64)
                outdata[burst.firstValidLine:burst.lastValidLine, burst.firstValidSample:burst.lastValidSample] =  data[burst.firstValidLine:burst.lastValidLine, burst.firstValidSample:burst.lastValidSample]

                ###################################################################################
                #Check if IPF version is 2.36 we need to correct for the Elevation Antenna Pattern 
                if burst.IPFversion == '002.36':
                   print('The IPF version is 2.36. Correcting the Elevation Antenna Pattern ...')
                   Geap = self.elevationAntennaPattern(burst)
                   for i in range(burst.firstValidLine, burst.lastValidLine):
                       outdata[i, burst.firstValidSample:burst.lastValidSample] = outdata[i, burst.firstValidSample:burst.lastValidSample]/Geap[burst.firstValidSample:burst.lastValidSample]
                ########################

                outdata.tofile(fid)
                fid.close()
               
                #Updated width and length to match extraction
                burst.numberOfSamples = width
                burst.numberOfLines = length

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

        band = None
        src = None

    def computeAzimuthCarrier(self, burst, offset=0.0, position=None):
        '''
        Returns the ramp function as a numpy array.
        '''
        Vs = np.linalg.norm(burst.orbit.interpolateOrbit(burst.sensingMid, method='hermite').getVelocity())
        Ks =   2 * Vs * burst.azimuthSteeringRate / burst.radarWavelength 


        if position is None:
            rng = np.arange(burst.numberOfSamples) * burst.rangePixelSize + burst.startingRange

## Seems to work best for basebanding data
            eta =( np.arange(0, burst.numberOfLines) - (burst.numberOfLines//2)) * burst.azimuthTimeInterval +  offset * burst.azimuthTimeInterval

            f_etac = burst.doppler(rng)
            Ka     = burst.azimuthFMRate(rng)

            eta_ref = (burst.doppler(burst.startingRange) / burst.azimuthFMRate(burst.startingRange) ) - (f_etac / Ka)

#            eta_ref *= 0.0
            Kt = Ks / (1.0 - Ks/Ka)


            carr = np.pi * Kt[None,:] * ((eta[:,None] - eta_ref[None,:])**2)

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

            carr = np.pi * Kt * ((eta - eta_ref)**2)

        return carr

    def elevationAntennaPattern(self,burst):

        eta_anx = burst.ascendingNodeTime        
        Ns = burst.numberOfSamples
        fs = burst.rangeSampleRate
        eta_start = burst.sensingStart
        tau0 = burst.slantRangeTime
        tau_sub = np.array(burst.slantRangeTimeSub)
        theta_sub = np.array(burst.elevationAngle)
        ###########################################
        #Reading the 2 way EAP (Elevation Antenna Pattern from AUX_CAL file)
        fp = open(burst.auxFile,'r')
        xml_root = ElementTree(file=fp).getroot()
        res = xml_root.find('calibrationParamsList/calibrationParams')
        paramsList = xml_root.find('calibrationParamsList')
        for par in (paramsList.getchildren()):
            if par.find('swath').text == burst.swath and par.find('polarisation').text == burst.polarization:
              print (par.find('swath').text)
              print (par.find('polarisation').text)
              delta_theta = float(par.find('elevationAntennaPattern/elevationAngleIncrement').text)
              Geap_IQ = [float(val) for val in par.find('elevationAntennaPattern/values').text.split()]
        I = np.array(Geap_IQ[0::2])
        Q = np.array(Geap_IQ[1::2])
        Geap = I[:]+Q[:]*1j   # Complex vector of Elevation Antenna Pattern
        Nelt = np.shape(Geap)[0]
        #########################
        # Vector of elevation angle in antenna frame
        theta_AM = np.arange(-(Nelt-1.)/2,(Nelt-1.)/2+1)*delta_theta        
        ########################
        delta_anx = (eta_start - eta_anx).total_seconds()
         
        theta_offnadir = anx2roll(delta_anx)
        theta_eap = theta_AM + theta_offnadir
        ########################
        #interpolate the 2-way complex EAP
        tau = tau0 + np.arange(Ns)/fs
        from scipy.interpolate import interp1d
       # f = interp1d(tau_sub,theta_sub)
       # theta = f(tau)
        theta = np.interp(tau, tau_sub, theta_sub)
       # theta = interpolate(theta_sub,tau_sub,tau)
        f2 = interp1d(theta_eap,Geap)
        Geap_interpolated = f2(theta) # interpolate(Geap,theta_eap,theta)
       # Geap_interpolated = np.interp(theta, theta_eap, Geap)
        phi_EAP = np.angle(Geap_interpolated)
        cJ = np.complex64(1.0j)
        GEAP = np.exp(cJ * phi_EAP)
        return GEAP
        ########################
        #correct each line of the burst 


    def computeRamp(self, burst, offset=0.0, position=None):
        '''
        Compute the phase ramp.
        '''
        cJ = np.complex64(1.0j)
        carr = self.computeAzimuthCarrier(burst,offset=offset, position=position)
        ramp = np.exp(-cJ * carr)
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

    def crop(self, bbox):
        '''
        Crop a given slice with a user provided bbox (SNWE).
        '''
        
        def overlap(box1,box2):
            '''
            Overlapping rectangles overlap both horizontally & vertically
            '''
            hoverlaps = True
            voverlaps = True
            
            if (box1[2] >= box2[3]) or (box1[3] <= box2[2]):
                hoverlaps = False

            if (box1[1] <= box2[0]) or (box1[0] >= box2[1]):
                voverlaps = False

            return hoverlaps and voverlaps


        cropList = []

        ###For each burst
        for ind, burst in enumerate(self.bursts):
            burstBox = burst.getBbox()

            #####If it overlaps, keep the burst
            if overlap(burstBox, bbox):
                print (burst.sensingStart,burst.sensingStop)
                cropList.append(burst)
            else:
                print('***************')
                print('No overlap:')
                print(ind)
                print (burst.sensingStart,burst.sensingStop)
                print('burstBox')
                print(burstBox)
                print('bbox')
                print(bbox)
                print('***************')
        
        self.numberBursts = len(cropList)
        self.bursts = None
        self.bursts = cropList

        return


def anx2roll(delta_anx):
   #Returns the Platform nominal roll as function of elapsed time from
   #ascending node crossing time (ANX)
      
   altitude = s1_anx2Height(delta_anx) 
   href=711.700 #;km
   boresight_ref= 29.450 # ; deg
   alpha_roll = 0.0566 # ;deg/km
   nominal_roll = boresight_ref - alpha_roll* (altitude/1000.0 - href)  #Theta off nadir
   return nominal_roll

def s1_anx2Height(delta_anx):
   
   h0 = 707714.8  #;m
   h = np.array([8351.5, 8947.0, 23.32, 11.74]) #;m
   phi = np.array([3.1495, -1.5655 , -3.1297, 4.7222]) #;radians
   Torb = (12*24*60*60)/175.
   worb = 2*np.pi / Torb
   sum=0
   for i in range(len(h)):
      sum += h[i] * np.sin((i+1) * worb * delta_anx + phi[i])

   Height = h0 + sum
   return Height
 

def createParser():
    import argparse

    parser = argparse.ArgumentParser( description = 'Sentinel parser' )

    parser.add_argument('-d', '--dirname', dest='dirname', type=str,
            default=None, help='SAFE format directory. (Recommended)')

    parser.add_argument('-s', '--swathnum', dest='swathnum', type=int,
            default=None, help='Swath number for analysis')

    parser.add_argument('-o', '--outdir', dest='outdir', type=str,
            required=True, help='Output SLC prefix.')

    parser.add_argument('-p', '--orbit', dest='orbit', type=str,
            default=None, help='Precise orbit file, Use of orbitdir preferred')

    parser.add_argument('-a', '--aux', dest='auxprod', type=str,
            default=None, help='Auxiliary product with antenna gains, Use of auxdir preferred')

    parser.add_argument('--orbitdir', dest='orbitdir', type=str,
            default=None, help = 'Directory with all the orbits')

    parser.add_argument('--auxdir', dest='auxdir', type=str,
            default=None, help = 'Directory with all the aux products')

    parser.add_argument('--pol', dest='polid', type=str,
            default='vv', help = 'Polarization of interest. Default: vv')

    parser.add_argument('-b', '--bbox', dest='bbox', type=str,
            default=None, help='Lat/Lon Bounding SNWE')
    return parser

def cmdLineParse(iargs=None):
    '''
    Command Line Parser.
    '''
    parser = createParser()
    inps = parser.parse_args(args=iargs)

    if inps.swathnum is None:
        raise Exception('Desired swath number is not provided')

    if inps.dirname is None:
        raise Exception('SAFE directory is not provided')

    inps.dirname = [x.strip() for x in inps.dirname.split()]
        
    ####First find annotation file
    swathid = 's1.-iw%d'%(inps.swathnum)
    swathid_re = re.compile(swathid)
    polid = inps.polid
    inps.xml = []

    for dirname in inps.dirname:
        for root, dirs, files in os.walk( os.path.join(dirname, 'annotation')):
            for fil in files:
                match = None
                if swathid_re.search(fil) and (polid in fil):
                    match = fil
#                    print('MATCH = ', match)
                    break

            if match is not None:
                break

        inps.xml.append(os.path.join(dirname, 'annotation', match))

    print('XML files: ', inps.xml)

    ####Find TIFF file
    inps.tiff = []
    for dirname in inps.dirname:
        for root, dirs, files in os.walk( os.path.join(dirname, 'measurement')):
            for fil in files:
                match = None
                if swathid_re.search(fil) and (polid in fil):
                    match = fil
                    break

            if match is not None:
                break

        inps.tiff.append(os.path.join(dirname, 'measurement', match))

    print('TIFF files: ', inps.tiff)


    ####Find manifest files
    inps.manifests = []
    for dirname in inps.dirname:
        inps.manifests.append(os.path.join(dirname, 'manifest.safe'))
    
    print('Manifest files: ', inps.manifests)


    ####Check bbox
    if inps.bbox is not None:
        inps.bbox = [float(x) for x in inps.bbox.split()]
        if len(inps.bbox) != 4:
            raise Exception('4 floats in SNWE format expected for bbox')

        if (inps.bbox[0] >= inps.bbox[1]) or (inps.bbox[2] >= inps.bbox[3]):
            raise Exception('Error in bbox definition: SNWE expected')

    return inps

def main(iargs=None):
    inps = cmdLineParse(iargs)

    slices = []
    relTimes = []
    burstWidths = []
    burstLengths = []
    numSlices = len(inps.tiff)

    ####Stage 1: Gather all the different slices
    for kk in range(numSlices): 
        obj = Sentinel1_TOPS()
        obj.configure()
        obj.xml = inps.xml[kk]
        obj.tiff = inps.tiff[kk]
        obj.manifest = inps.manifests[kk]
        obj.outdir = inps.outdir
        obj.orbitFile = inps.orbit
        obj.auxFile = inps.auxprod
        obj.auxDir = inps.auxdir
        obj.orbitDir = inps.orbitdir
        obj.parse()
        if kk == 0:
           dt = (obj.bursts[1].sensingStart - obj.bursts[0].sensingStart).total_seconds()  # assuming dt is constant! should be checked

        if inps.bbox is not None:
            obj.crop(inps.bbox)

        if obj.numberBursts != 0:
            ##Add to list of slices
            slices.append(obj)
            relTimes.append((obj.bursts[0].sensingStart - slices[0].bursts[0].sensingStart).total_seconds())
            burstWidths.append(obj.bursts[0].numberOfSamples)
            burstLengths.append(obj.bursts[0].numberOfLines)
    
    ###Adjust the number of slices to account for cropping
    numSlices = len(slices)

    if numSlices == 0:
        raise Exception('No bursts left to process')
    elif numSlices == 1:
        obj = slices[0]
        obj.extractImage(parse=False)
    else:
        print('Stitching slices')
        indices = np.argsort(relTimes)
        commonWidth = max(burstWidths)
        commonLength = max(burstLengths)
        firstSlice = slices[indices[0]]

        t0 = firstSlice.bursts[0].sensingStart
      #  if len(firstSlice.bursts)==1:   
      #     dt = (firstSlice.bursts[0].sensingStop - t0).total_seconds()
      #  else:
      #     dt = (firstSlice.bursts[1].sensingStart - t0).total_seconds()

        obj = Sentinel1_TOPS()
        obj.xml = inps.xml
        obj.tiff = inps.tiff
        obj.outdir = inps.outdir
        obj.orbitFile = inps.orbit
        obj.orbitDir = inps.orbitdir
        obj.auxDir = inps.auxdir
        obj.auxFile = inps.auxprod
        obj.IPFversion = firstSlice.IPFversion

        for index in indices:
            slc = slices[index]
            print('slc.numberBursts')
            print (slc.numberBursts)
            offset = np.int(np.rint((slc.bursts[0].sensingStart - t0).total_seconds()/dt))
            slc.extractImage(parse=False, nameOffset=offset,
                    width=commonWidth, length=commonLength)
            print ('offset: ',offset)
            for kk in range(slc.numberBursts):
                ###Overwrite previous copy if one exists
                
                if (offset + kk) < len(obj.bursts):
                    print('Overwrite previous copy')
                    print (obj.bursts[offset+kk].sensingStart,obj.bursts[offset+kk].sensingStop)
                    print (slc.bursts[kk].sensingStart,slc.bursts[kk].sensingStop)
                    obj.bursts[offset+kk] = slc.bursts[kk]
                ###Else append new burst
                elif (offset+kk) == len(obj.bursts):
                    obj.bursts.append(slc.bursts[kk])
                else:
                    print('Offset indices = ', indices)
                    raise Exception('There seems to be a gap between slices.')
        
        obj.numberBursts = len(obj.bursts)
        print(obj.numberBursts)
        ####Reparsing the orbit file 
        obj.orbitFile = firstSlice.orbitFile
        if obj.orbitFile is None:
            raise Exception('Need restituted / precise orbits for stitching slices')
        else:
            orb = obj.extractPreciseOrbit()

            for burst in obj.bursts:
                burst.orbit = Orbit()
                burst.orbit.configure()
                burst.orbit.setOrbitSource('Header')

                for sv in orb:
                    burst.orbit.addStateVector(sv)


    sname = os.path.join(inps.outdir, 'data')

    ###Reindex all the bursts for later use
    for ind, burst in enumerate(obj.bursts):
        burst.burstNumber = ind+1
    
    with shelve.open(os.path.join(inps.outdir, 'data')) as db:
        db['swath'] = obj

if __name__ == '__main__':

    main()

