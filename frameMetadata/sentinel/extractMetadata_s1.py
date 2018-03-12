#!/usr/bin/env python3

import isce
from isceobj.Scene.Frame import Frame
from isceobj.Planet.AstronomicalHandbook import Const
from isceobj.Planet.Planet import Planet

from Sentinel1_TOPS import Sentinel1_TOPS
import argparse
import os, re
from lxml import objectify as OBJ
from FrameInfoExtractor import FrameInfoExtractor as FIE


DATASETTYPE_RE = re.compile(r'-(raw|slc)-')

MISSION_RE = re.compile(r'S1(\w)')


def cmdLineParse():
    '''
    Command line parsing.
    '''

    parser = argparse.ArgumentParser(description='Extract metadata from S1 swath')
    parser.add_argument('-i','--input', dest='inxml', type=str, required=True,
            help='Swath XML file')
    parser.add_argument('-o', '--output', dest='outjson', type=str, required=True,
            help = 'Ouput met.json')
    return parser.parse_args()

def objectify(inxml):
    '''
    Return objectified XML.
    '''
    with open(inxml, 'r') as fid:
        root = OBJ.parse(fid).getroot()
    return root

def getGeometry(obj):
    '''
    Get bbox and central coordinates.
    '''
    pts = []
    glist = obj.geolocationGrid.geolocationGridPointList

    for child in glist.getchildren():
        pts.append( [float(child.line), float(child.pixel), float(child.latitude), float(child.longitude)])

    ys = sorted(list(set([x[0] for x in pts])))
    dy = ys[1] - ys[0]
    ny= int((ys[-1] - ys[0])/dy + 1)

    xs = sorted(list(set([x[1] for x in pts])))
    dx = xs[1] - xs[0]
    nx = int((xs[-1] - xs[0])/dx + 1)

    lat = np.array([x[2] for x in pts]).reshape((ny,nx))
    lon = np.array([x[3] for x in pts]).reshape((ny,nx))

    bbox = [[lat[0,0],lon[0,0]], [lat[0,-1],lon[0,-1]],
            [lat[-1,-1],lon[-1,-1]], [lat[-1,0], lon[-1,0]]]

    center = { "coordinates": [lon[ny//2,nx//2], lat[ny//2, nx//2]],
               "type" : "point"}

    return center, bbox


class S1toFrame(object):
    '''
    Create a traditional ISCE Frame object from S1 container.
    '''

    def __init__(self, sar, obj):
        self.sar = sar
        self.obj = obj
        self.missionId = self.obj.xpath('.//missionId/text()')[0]
        self.missionId_char = MISSION_RE.search(self.missionId).group(1)
        self.frame = Frame()
        self.frame.configure()

        self.parse()

    def parse(self):
        self._populatePlatform()
        self._populateInstrument()
        self._populateFrame()
        self._populateOrbit()
        self._populateExtras()

    def _populatePlatform(self):
        platform = self.frame.getInstrument().getPlatform()
        platform.setMission(self.missionId)
        platform.setPlanet(Planet(pname='Earth'))
        platform.setPointingDirection(-1)
        platform.setAntennaLength(40.0)
        
    def _populateInstrument(self):
        ins = self.frame.getInstrument()
        b0 = self.sar.bursts[0]
        b1 = self.sar.bursts[-1]

        ins.setRadarWavelength(b0.radarWavelength)
        ins.setPulseRepetitionFrequency(1.0/b0.azimuthTimeInterval)
        ins.setRangePixelSize(b0.rangePixelSize)

        tau = self.obj.generalAnnotation.replicaInformationList.replicaInformation.referenceReplica.timeDelay
        ins.setPulseLength(float(tau))
        slope = str(self.obj.generalAnnotation.replicaInformationList.replicaInformation.referenceReplica.phaseCoefficients).split()[2]
        ins.setChirpSlope(float(slope))
        
        fsamp = Const.c / (2.0 * b0.rangePixelSize)
        ins.setRangeSamplingRate(fsamp)

        ins.setInPhaseValue(127.5)
        ins.setQuadratureValue(127.5)
        ins.setBeamNumber(self.obj.adsHeader.swath)
       
    def _populateFrame(self):
        frame = self.frame
        b0 = self.sar.bursts[0]
        b1 = self.sar.bursts[-1]


        hdg = self.obj.generalAnnotation.productInformation.platformHeading
        if hdg < -90:
            frame.setPassDirection('Descending')
        else:
            frame.setPassDirection('Ascending')

        frame.setStartingRange(b0.startingRange)
        frame.setOrbitNumber(int(self.obj.adsHeader.absoluteOrbitNumber))
        frame.setProcessingFacility('Sentinel 1%s' % self.missionId_char)
        frame.setProcessingSoftwareVersion('IPF')
        frame.setPolarization(self.obj.adsHeader.polarisation)
        frame.setNumberOfSamples(int(self.obj.imageAnnotation.imageInformation.numberOfSamples))
        frame.setNumberOfLines(int(self.obj.imageAnnotation.imageInformation.numberOfLines))
        frame.setSensingStart(b0.sensingStart)
        frame.setSensingStop(b1.sensingStop)

        tmid = b0.sensingStart + 0.5 * (b1.sensingStop - b0.sensingStart)
        frame.setSensingMid(tmid)
        
        farRange = b0.startingRange + frame.getNumberOfSamples() * b0.rangePixelSize
        frame.setFarRange(farRange)

    def _populateOrbit(self):
        b0 = self.sar.bursts[0]
        self.frame.orbit = b0.orbit


    def _populateExtras(self):
        b0 = self.sar.bursts[0]
        self.frame._squintAngle = 0.0
        self.frame.doppler = b0.doppler._coeffs[0]
        match = DATASETTYPE_RE.search(self.sar.xml)
        if match: self.frame.datasetType = 'slc'
        else: self.frame.datasetType = ''


if __name__ == '__main__':
    '''
    Main driver.
    '''
    
    #Parse command line
    inps = cmdLineParse()

    #Read in metadata
    sar = Sentinel1_TOPS()
    sar.xml = inps.inxml
    sar.parse()
    obj = objectify(inps.inxml)
    
    ####Copy into ISCE Frame
    frame = S1toFrame(sar,obj)

    ####Frameinfoextractor
    fie = FIE()
    frameInfo = fie.extractInfoFromFrame(frame.frame)

    frameInfo.dump(inps.outjson)
