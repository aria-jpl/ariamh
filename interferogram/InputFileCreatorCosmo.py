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
class InputFileCreatorCosmo(InputFileCreator):
    def __init__(self):
        super(InputFileCreatorCosmo,self).__init__()
        
   
    def createInputFile(self,listFiles,jobJson):
        self.init(jobJson)                   
        fp1 = open(os.path.join(os.path.dirname(__file__),'insarCosmoTemplate.xml'))
        lines = fp1.readlines()
        fp1.close()
        fp = open(self._fileOut,'w')

        for line,i in zip(lines,list(range(len(lines)))):
            #technically the presence of only one of the uu attributes is sufficient
            #but this is more robust
            if(i == len(lines) - 2):
                self.addCommons(fp)
                
            self.readCommonCosmo(line,listFiles, fp)
                     
        fp.close()
    def readCommonCosmo(self,line,listFiles,fp):
        
        if line.count('masterh5value'):
            fp.write(line.replace('masterh5value',str(listFiles[0][0])))               
        elif line.count('masteroutputvalue'):
            fp.write(line.replace('masteroutputvalue',str(listFiles[0][1])))
        elif line.count('slaveh5value'):
            fp.write(line.replace('slaveh5value',str(listFiles[1][0])))
        elif line.count('slaveoutputvalue'):
            fp.write(line.replace('slaveoutputvalue',str(listFiles[1][1])))
        else:
            fp.write(line)    
            
   
        
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
