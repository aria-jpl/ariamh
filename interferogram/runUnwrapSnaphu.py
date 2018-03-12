#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Piyush Agram
# Copyright 2013, by the California Institute of Technology. ALL RIGHTS
# RESERVED. United States Government Sponsorship acknowledged. Any commercial
# use must be negotiated with the Office of Technology Transfer at the
# California Institute of Technology.
#
# This software may be subject to U.S. export control laws. By accepting this
# software, the user agrees to comply with all applicable U.S. export laws and
# regulations. User has the responsibility to obtain export licenses, or other
# export authority as may be required before exporting such information to
# foreign countries or providing access to foreign persons.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# giangi: taken Piyush code for snaphu and adapted

import sys
import isceobj
from contrib.Snaphu.Snaphu import Snaphu
from isceobj.Constants import SPEED_OF_LIGHT
def runUnwrap(self,costMode = None,initMethod = None, defomax = None, initOnly = None):

    if costMode is None:
        costMode   = 'DEFO'
    
    if initMethod is None:
        initMethod = 'MST'
    
    if  defomax is None:
        defomax = 4.0
    
    if initOnly is None:
        initOnly = False
    
    

    wavelength = self.insar.masterFrame.getInstrument().getRadarWavelength()
    width      = self.insar.resampIntImage.width 
    earthRadius = self.insar.peg.radiusOfCurvature 
    altitude   = self.insar.averageHeight
    corrfile  = self.insar.getCoherenceFilename()
    rangeLooks = self.insar.topo.numberRangeLooks
    azimuthLooks = self.insar.topo.numberAzimuthLooks

    azres = self.insar.masterFrame.platform.antennaLength/2.0
    azfact = self.insar.topo.numberAzimuthLooks *azres / self.insar.topo.azimuthSpacing

    rBW = self.insar.masterFrame.instrument.pulseLength * self.insar.masterFrame.instrument.chirpSlope
    rgres = abs(SPEED_OF_LIGHT / (2.0 * rBW))
    rngfact = rgres/self.insar.topo.slantRangePixelSpacing

    corrLooks = self.insar.topo.numberRangeLooks * self.insar.topo.numberAzimuthLooks/(azfact*rngfact) 
    maxComponents = 20
    #Unfortunately isce changes topophaseFlatFilename to the filtered one so need to change
    #it back 
    self._unwrapList = [(self.insar.topophaseFlatFilename.replace('filt_',''),self.insar.topophaseFlatFilename.replace('.flat','.unw').replace('filt_','')),
                            (self.insar.filt_topophaseFlatFilename,self.insar.filt_topophaseFlatFilename.replace('.flat','.unw'))]
    
    #need to add them to the instance so they can later be part of the geolist
    self._unwrappedIntFilename = self._unwrapList[0][1]
    self._unwrappedIntFiltFilename = self._unwrapList[1][1]

    for wrapName,unwrapName in self._unwrapList:
        snp = Snaphu()
        snp.setInitOnly(initOnly)
        snp.setInput(wrapName)
        snp.setOutput(unwrapName)
        snp.setWidth(width)
        snp.setCostMode(costMode)
        snp.setEarthRadius(earthRadius)
        snp.setWavelength(wavelength)
        snp.setAltitude(altitude)
        snp.setCorrfile(corrfile)
        snp.setInitMethod(initMethod)
        snp.setCorrLooks(corrLooks)
        snp.setMaxComponents(maxComponents)
        snp.setDefoMaxCycles(defomax)
        snp.setRangeLooks(rangeLooks)
        snp.setAzimuthLooks(azimuthLooks)
        snp.prepare()
        snp.unwrap()
    
        ######Render XML
        outImage = isceobj.Image.createUnwImage()
        outImage.setFilename(unwrapName)
        outImage.setWidth(width)
        outImage.setAccessMode('read')
        outImage.createImage()
        outImage.renderHdr()
        outImage.finalizeImage()
    
     

    return
def runUnwrapMcf(self):
    runUnwrap(self,costMode = 'SMOOTH',initMethod = 'MCF', defomax = 2, initOnly = True)
    return
