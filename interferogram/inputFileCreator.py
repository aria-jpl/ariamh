#! /usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
from __future__ import print_function
import sys
#sensor is the sensor name since each input file will be different depending on that
#listFiles is a list of lists of lists ([0] for master [1] for slave) of files necessary to create the inputFile it's sensor dependent
#filenOut is the name of the input file created. if not specified defaults to 'extractorInput.xml'
#unwrap since it's very costly make as an option, specially when debugging
#useHighRes it's a flag to see if one should trying only if high res dem
class InputFileCreator(object):
    def __init__(self):
        self._fileOut = ""
        self._unwrap = None
        self._useHighRes = None
    def getFileOut(self):
        return self._fileOut

    def createInputFile(self,sensor,listFiles,fileOut = None,unwrap = None, useHighRes = None):
        if(fileOut is None):
            self._fileOut = 'insarMH.xml'
        else:
            self._fileOut = fileOut
        if(unwrap is None):
            self._unwrap = 'True'
        else:
            self._unwrap = unwrap
        if(useHighRes is None):
            self._useHighRes = 'False'
        else:
            self._useHighRes = useHighRes 
        fp = open(self._fileOut,'w')
        if(sensor.upper() == 'ALOS'):
            #for alos listFiles is [0] IMG, [1] LED, [2] output raw filename
            strToSave = '<?xml version="1.0" encoding="UTF-8"?> \n' + \
                        '<insarMH>\n' + \
                        '    <component name="insarMH"> \n' + \
                        '        <property name="Sensor Name"> \n' + \
                        '            <value>' + sensor.upper() + '</value> \n' + \
                        '        </property> \n' + \
                        '        <property name="unwrap"> \n' + \
                        '            <value>' + str(self._unwrap) +'</value> \n' + \
                        '        </property> \n' + \
                        '        <property name="useHighResolutionDem"> \n' + \
                        '            <value>' + str(self._useHighRes) +'</value> \n' + \
                        '        </property> \n' + \
                        '        <component name="Master"> \n' + \
                        '            <property name="LEADERFILE"> \n' + \
                        '                <value>' + str(listFiles[0][1]) +'</value> \n' + \
                        '            </property> \n' + \
                        '            <property name="IMAGEFILE"> \n' + \
                        '                <value>' + str(listFiles[0][0]) +'</value> \n' + \
                        '            </property> \n' + \
                        '            <property name="OUTPUT"> \n' + \
                        '                <value>' + listFiles[0][2] +'</value> \n' + \
                        '            </property> \n' + \
                        '        </component> \n' + \
                        '        <component name="Slave"> \n' + \
                        '            <property name="LEADERFILE"> \n' + \
                        '                <value>' + str(listFiles[1][1]) +'</value> \n' + \
                        '            </property> \n' + \
                        '            <property name="IMAGEFILE"> \n' + \
                        '                <value>' + str(listFiles[1][0]) +'</value> \n' + \
                        '            </property> \n' + \
                        '            <property name="OUTPUT"> \n' + \
                        '                <value>' + listFiles[1][2] +'</value> \n' + \
                        '            </property> \n' + \
                        '        </component> \n' + \
                        '    </component> \n' + \
                        '</insarMH> \n'
            fp.write(strToSave)
        fp.close()
def main():
    listIn = [[['IMG-HH-ALPSRP225250640-H1.0__A','IMG-HH-ALPSRP225250630-H1.0__A'],['LED-ALPSRP225250640-H1.0__A','LED-ALPSRP225250630-H1.0__A'],'master.raw'],
              [['IMG-HH-ALPSRP225250620-H1.0__A','IMG-HH-ALPSRP225250610-H1.0__A'],['LED-ALPSRP225250620-H1.0__A','LED-ALPSRP225250610-H1.0__A'],'slave.raw']]
    sensor = "Alos"
    createInputFile(sensor,listIn)
if __name__ == '__main__':
    sys.exit(main())
