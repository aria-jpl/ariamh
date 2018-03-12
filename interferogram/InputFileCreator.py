#! /usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import sys
import os
import isce
from isceobj.InsarProc.InsarProc import InsarProc
#sensor is the sensor name since each input file will be different depending on that
#listFiles is a list of lists of lists ([0] for master [1] for slave) of files necessary to create the inputFile it's sensor dependent
#filenOut is the name of the input file created. if not specified defaults to 'extractorInput.xml'
#unwrap since it's very costly make as an option, specially when debugging
#useHighRes it's a flag to see if one should trying only if high res dem
class InputFileCreator(object):
    def __init__(self):
        self._fileOut = 'insar.xml'
        self._unwrap = None
        self._unwrapper_name = None
        self._useHighRes = 'True'
        self._filterStrength = None
        self._addStitcher = True
        self._demFile = None
        self._posting= None
        self._geoList = None
        self._insar = InsarProc()
        self._insar.configure()
    def getFileOut(self):
        return self._fileOut
    def init(self,jobJson):
        self._unwrap = jobJson['unwrap']
        self._unwrapper_name = jobJson['unwrapper']
        self._filterStrength = jobJson['filterStrength']
        self._posting = jobJson['posting']
        self._geoList = self.getGeoList(jobJson['createInterferogram']['geolist'])
        if('demFile' in jobJson):
            self._addStitcher = False
            self._demFile = jobJson['demFile']
    
    def getGeoList(self,geolist):
        ret = []
        for geo in geolist:
            ret.append(geo)
        return ret
        
    def createInputFile(self,listFiles,jobJson):
        
        #project = None,fileOut = None,unwrap = None, useHighRes = None, unwrapper_name=None
        raise NotImplementedError("Need to implement the createInputFile method")
    
    def addCommons(self,fp):
        fp1 = open(os.path.join(os.path.dirname(__file__),'commonTemplate.xml'))
        extraLines = fp1.readlines()
        fp1.close()
        for line in extraLines:
            if line.count('unwrapvalue'):
                fp.write(line.replace('unwrapvalue',str(self._unwrap)))
            elif line.count('unwrappervalue'):
                fp.write(line.replace('unwrappervalue',str(self._unwrapper_name)))
            elif line.count('posting'):
                fp.write(line.replace('posting',str(self._posting)))     
            elif line.count('useHighResolutionDemOnlyvalue'):
                fp.write(line.replace('useHighResolutionDemOnlyvalue',str(self._useHighRes)))
            elif line.count('filtervalue'):
                fp.write(line.replace('filtervalue',str(self._filterStrength)))
            elif line.count('geolistvalue'):
                fp.write(line.replace('geolistvalue',str(self._geoList)))
            else:
                fp.write(line)   
        fp.write('\n')
        if(self._addStitcher):          
            self.addStitcher(fp)
        else:
            self.addDem(fp)
     
    def addDem(self,fp):
        towrite = '        <component name=\'Dem\'>\n' +\
                  '            <catalog>' + self._demFile +'</catalog>\n' + \
                  '        </component>\n'
        fp.write(towrite)        
    def addStitcher(self,fp):
        from utils.UrlUtils import UrlUtils
        uu = UrlUtils()
        fp1 = open(os.path.join(os.path.dirname(__file__),'demstitchertemplate.xml'))
        extraLines = fp1.readlines()
        fp1.close()
        for el in extraLines:
            if el.count('httpsvalue'):
                fp.write(el.replace('httpsvalue',str(uu.dem_url)))
            elif el.count('usernamevalue'):
                fp.write(el.replace('usernamevalue',str(uu.dem_u)))
            elif el.count('passwordvalue'):
                fp.write(el.replace('passwordvalue',str(uu.dem_p))) 
            else:
                fp.write(el)
        fp.write('\n')
        
def main():
    ifc = InputFileCreator()
    #listIn = [[['IMG-HH-ALPSRP225250640-H1.0__A','IMG-HH-ALPSRP225250630-H1.0__A'],['LED-HH-ALPSRP225250640-H1.0__A','LED-HH-ALPSRP225250630-H1.0__A'],'master.raw'],
    #          [['IMG-HH-ALPSRP225250620-H1.0__A','IMG-HH-ALPSRP225250610-H1.0__A'],['LED-HH-ALPSRP225250620-H1.0__A','LED-HH-ALPSRP225250610-H1.0__A'],'slave.raw']]
    #sensor = "alos"
    sensor = "cosmo_skymed"
    listIn = [[['CSKS1_RAW_B_HI_04_HH_RA_SF_20130609132512_20130609132519.h5'],'master.raw'],
              [['CSKS1_RAW_B_HI_04_HH_RA_SF_20130828132440_20130828132447.h5'],'slave.raw']]
    ifc.createInputFile(sensor,listIn)
if __name__ == '__main__':
    sys.exit(main())
