#!/usr/bin/env python3

import isce
import datetime
import isceobj
import numpy as np
from isceobj.Attitude.Attitude import Attitude
from iscesys.Component.Component import Component
from isceobj.Image.Image import Image
from isceobj.Orbit.Orbit import Orbit
from isceobj.Util.decorators import type_check

class BurstSLC(Component):
    """A class to represent a burst SLC along a radar track"""
    
    family = 'burstslc'
    logging_name = 'isce.burstslc'

    def __init__(self,name=''):
        super(BurstSLC, self).__init__(family=self.__class__.family, name=name)
        self.numberOfSamples = None
        self.numberOfLines   = None
        self.startingRange   = None
        self.sensingStart    = None
        self.sensingStop     = None
        self.startUTC        = None
        self.stopUTC         = None
        self.midUTC          = None
        self.sensingMid      = None
        self.trackNumber     = None
        self.frameNumber     = None
        self.orbitNumber     = None
        self.passDirection   = None
        self.azimuthSteeringRate = None
        self.azimuthFMRate   = None
        self.azimuthTimeInterval = None
        self.rangePixelSize  = None
        self.radarWavelength = None
        self.doppler         = None
        self.swath           = None
        self.polarization    = None
        self.terrainHeight   = None
        self.prf             = None
        self.image           = None
        self.derampimage     = None
        self.firstValidLine  = None
        self.numValidLines   = None
        self.firstValidSample = None
        self.numValidSamples = None
        self.ascendingNodeTime = None
        self.burstNumber = None
        return None


    def _facilities(self):
        '''
        Defines all the user configurable facilities for this application.
        '''

        self.orbit = self.facility(
                'orbit',
                public_name='ORBIT',
                module = 'isceobj.Orbit.Orbit',
                factory = 'createOrbit',
                args=(),
                mandatory=True,
                doc = "Orbit information")

    @property
    def lastValidLine(self):
        return self.firstValidLine + self.numValidLines

    @property
    def lastValidSample(self):
        return self.firstValidSample + self.numValidSamples

    def getBbox(self ,hgtrange=[-500,9000]):
        '''
        Bounding box estimate.
        '''

        ts = [self.sensingStart, self.sensingStop]
        rngs = [self.startingRange, self.startingRange + self.numberOfSamples * self.rangePixelSize]
       
        pos = []
        for ht in hgtrange:
            for tim in ts:
                for rng in rngs:
                    llh = self.orbit.rdr2geo(tim, rng, height=ht)
                    pos.append(llh)

        pos = np.array(pos)

        bbox = [np.min(pos[:,0]), np.max(pos[:,0]), np.min(pos[:,1]), np.max(pos[:,1])]
        return bbox
