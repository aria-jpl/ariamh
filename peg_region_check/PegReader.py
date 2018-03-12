#! /usr/bin/env python3
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2011, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


import os
import sys
from isceobj.Location.Peg import PegFactory
from isceobj.Planet.Planet import Planet
from isceobj.Location.Coordinate import Coordinate
import logging
import logging.config
logging.config.fileConfig(
    os.path.join(os.environ['ISCE_HOME'], 'defaults', 'logging',
        'logging.conf')
)
class PegReader:


    # if the file has a different structure from the following, then set a list that does the mapping of the positions.
    # the position 'i' of the list has to contain the index where the record corresponding to the default    # sequence is now found. for instance if PagBandIndx is not in position 4 (zero based)
    # then self._listIndx[0] = 4

    # default list: 0 = PegBandIndx, 1 = PathNo (i.e. TrackNumebr), 2 = Direction (asc,dsc),3  = LatStart,
    # 4 = LatEnd, 5 = PegLat, 6 = PegLon, 7 = PegHeading
    def setListIndex(self,indx):
        self._listIndex = indx

    def createPegList(self,filename = None):
        self._pegList = []
        if not filename:
            if self._inputfile == '':
                self.logger.error('Error, a filename must be set.')
                raise Exception
            filename = self._inputfile
        fp = open(filename)
        allLines = fp.readlines()
        for line in allLines[1:]:# first line just record specification ... skip it
            if not line: continue
            lineS = line.split()
            if len(lineS) < len(self._listIndex): # there must be at least so many records in the file
                self.logger.error('Error, the list of indices provided must have at least',len(self._listIndex),'elements')
                raise Exception
            PI = PegInfoFactory.createPegInfo(lineS[self._listIndex[0]],int(lineS[self._listIndex[1]]), \
                                         lineS[self._listIndex[2]],float(lineS[self._listIndex[3]]), \
                                         float(lineS[self._listIndex[4]]),float(lineS[self._listIndex[5]]), \
                                         float(lineS[self._listIndex[6]]),float(lineS[self._listIndex[7]]))

            self._pegList.append(PI)
        return self._pegList

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['logger']
        return d
    def __setstate__(self,d):
        self.__dict__.update(d)
        self.logger = logging.getLogger("PegReader")

    def __init__(self,filename = None):

        self.logger = logging.getLogger("PegReader")
        self._inputfile = ''
        if not filename:
            self._inputfile = filename

        self._pegList = None
        self._listIndex = [i for i in range(8)]

class PegInfo:

    def __init__(self):

        self._pegBandIndx = None
        self._track = None
        self._direction = ''
        self._latStart = None
        self._latEnd = None
        self._peg = None

    def getPegBandIndx(self):
        return self._pegBandIndx
    def getTrack(self):
        return self._track
    def getDirection(self):
        return self._direction
    def getLatStart(self):
        return self._latStart
    def getLatEnd(self):
        return self._latEnd
    def getPeg(self):
        return self._peg

    def setPegBandIndx(self,val):
        self._pegBandIndx = val
    def setTrack(self,val):
        self._track = val
    def setDirection(self,val):
        self._direction = val
    def setLatStart(self,val):
        self._latStart = val
    def setLatEnd(self,val):
        self._latEnd = val
    def setPeg(self,val):
        self._peg = val


    pegBandIndx = property(getPegBandIndx,setPegBandIndx)
    track = property(getTrack,setTrack)
    direction = property(getDirection,setDirection)
    latStart = property(getLatStart,setLatStart)
    latEnd = property(getLatEnd,setLatEnd)
    peg = property(getPeg,setPeg)

class PegInfoFactory:
    @staticmethod
    def createPegInfo(band,track,dire,latS,latE,pegLat,pegLon,hdg):
        PI = PegInfo()
        PI._pegBandIndx = band
        PI._track = track
        PI._direction = dire
        PI._latStart = latS
        PI._latEnd = latE
        PI._peg = PegFactory.fromEllipsoid(Coordinate(pegLat,pegLon),hdg,PegInfoFactory._Planet.get_elp())
        return PI

    _Planet = Planet(pname='Earth')


def main():
    import pdb
    pdb.set_trace()
    PR = PegReader()
    pegL = PR.createPegList(sys.argv[1])
    fp = open('testPeg','w')
    fp.write("#title")
    for peg in pegL:
        fp.write(str(peg.pegBandIndx) + '\t' + str(peg.track) + '\t' +str(peg.direction) + '\t' +str(peg.latStart) + '\t' +str(peg.latEnd) + '\t' + \
                str(peg.peg.latitude) + '\t' +str(peg.peg.longitude) + '\t' +str(peg.peg.heading) + '\n' )
    fp.close()
if __name__ == "__main__":
    sys.exit(main())
