#! /usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import sys
import os
from interferogram.InputFileCreator import InputFileCreator

#sensor is the sensor name since each input file will be different depending on that
#listFiles is a list of lists of lists ([0] for master [1] for slave) of files necessary to create the inputFile it's sensor dependent
#filenOut is the name of the input file created. if not specified defaults to 'extractorInput.xml'
#unwrap since it's very costly make as an option, specially when debugging
#useHighRes it's a flag to see if one should trying only if high res dem
class InputFileCreatorAlos(InputFileCreator):
    def __init__(self):
        super(InputFileCreatorAlos,self).__init__()
   
    
    def createInputFile(self,listFiles,project = None,fileOut = None,unwrap = None, useHighRes = None, unwrapper_name=None):
        from utils.UrlUtils import UrlUtils
        uu = UrlUtils()
        if(fileOut is None):
            self._fileOut = 'insar.xml'
        else:
            self._fileOut = fileOut
        if(unwrap is None):
            self._unwrap = 'True'
        else:
            self._unwrap = unwrap
        if(useHighRes is None):
            self._useHighRes = 'True'
        else:
            self._useHighRes = useHighRes
        if(self._unwrapper_name is None):
            self._unwrapper_name = 'icu'
        else:
            self._unwrapper_name = unwrapper_name
        fp = open(self._fileOut,'w')
        fp1 = open(os.path.join(os.path.dirname(__file__),'insarAlosTemplate.xml'))
        lines = fp1.readlines()
        fp1.close()
        for line,i in zip(lines,list(range(len(lines)))):
            #technically the presence of only one of the uu attributes is sufficient
            #but this is more robust
            if(i == len(lines) - 2) and uu.dem_p and uu.dem_u and uu.dem_url:
                self.addStitcher(fp, uu)
            if line.count('unwrapvalue'):
                fp.write(line.replace('unwrapvalue',str(self._unwrap)))
            elif line.count('unwrappervalue'):
                fp.write(line.replace('unwrappervalue',str(self._unwrapper_name)))    
            elif line.count('useHighResolutionDemOnlyvalue'):
                fp.write(line.replace('useHighResolutionDemOnlyvalue',str(self._useHighRes)))
            elif line.count('masterimgvalue'):
                fp.write(line.replace('masterimgvalue',str(listFiles[0][0])))
            elif line.count('masterledvalue'):
                fp.write(line.replace('masterledvalue',str(listFiles[0][1])))                
            elif line.count('masteroutputvalue'):
                fp.write(line.replace('masteroutputvalue',str(listFiles[0][2])))
            elif line.count('slaveimgvalue'):
                fp.write(line.replace('slaveimgvalue',str(listFiles[1][0])))
            elif line.count('slaveledvalue'):
                fp.write(line.replace('slaveledvalue',str(listFiles[1][0])))
            elif line.count('slaveoutputvalue'):
                fp.write(line.replace('slaveoutputvalue',str(listFiles[1][2])))
            else:
                fp.write(line)    
        fp.close()
          
                
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
