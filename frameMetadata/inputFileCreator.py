#! /usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import sys
#sensor is the sensor name since each input file will be different depending on that
#listFiles is the list of files necessary to create the inputFile it's sensor dependent
#filenOut is the name of the input file created. if not specified defaults to 'extractorInput.xml'
def createInputFile(argv,fileOut = None):
    if(fileOut is None):
        fileOut = 'extractorInput.xml'
    fp = open(fileOut,'w')
    sensor = argv[0]
    listFiles = argv[1:]
    #the input for alos and alos2 are the same so just make sure that alos is 
    #part of the str sensor
    if(sensor.upper().count('ALOS')):
        #for alos listFiles is [0] IMG, [1] LED, [2] output raw filename
        strToSave = '<?xml version="1.0" encoding="UTF-8"?> \n' + \
                    '<extractMetadata>\n' + \
                    '    <component name="extractMetadata"> \n' + \
                    '        <property name="Sensor Name"> \n' + \
                    '            <value>' + sensor.upper() + '</value> \n' + \
                    '        </property> \n' + \
                    '        <property name="datasetType"> \n' + \
                    '            <value>raw</value> \n' + \
                    '        </property> \n' + \
                    '        <property name="metadata file"> \n' + \
                    '            <value>' + listFiles[0] +'.json</value> \n' + \
                    '        </property> \n' + \
                    '        <property name="doppler method"> \n' +\
                    '            <value>useDEFAULT</value> \n' +\
                    '        </property> \n' +\
                    '        <component name="sensor"> \n' + \
                    '            <property name="LEADERFILE"> \n' + \
                    '                <value>' + listFiles[1] +'</value> \n' + \
                    '            </property> \n' + \
                    '            <property name="IMAGEFILE"> \n' + \
                    '                <value>' + listFiles[0] +'</value> \n' + \
                    '            </property> \n' + \
                    '            <property name="OUTPUT"> \n' + \
                    '                <value>' + listFiles[2] +'</value> \n' + \
                    '            </property> \n' + \
                    '        </component> \n' + \
                    '    </component> \n' + \
                    '</extractMetadata> \n'
        fp.write(strToSave)
    if(sensor == 'CSK'):
        #for csk listFiles is [0] HDF5,  [1] output raw filename
        if 'RAW' in listFiles[0]:
            sensorName = 'COSMO_SkyMed'
            dataType = 'raw'
        elif 'SCS' in listFiles[0]:
            sensorName = 'COSMO_SkyMed_SLC'
            dataType = 'slc'
        else:
            raise ValueError('Unknown HDF5 file')

        strToSave = '<?xml version="1.0" encoding="UTF-8"?> \n' + \
                    '<extractMetadata>\n' + \
                    '    <component name="extractMetadata"> \n' + \
                    '        <property name="Sensor Name"> \n' + \
                    '            <value>' + sensorName + '</value> \n' + \
                    '        </property> \n' + \
                     '        <property name="datasetType"> \n' + \
                    '            <value>'+dataType+'</value> \n' + \
                    '        </property> \n' + \
                    '        <property name="metadata file"> \n' + \
                    '            <value>' + listFiles[0] +'.json</value> \n' + \
                    '        </property> \n' + \
                    '        <property name="doppler method"> \n' +\
                    '            <value>useDEFAULT</value> \n' +\
                    '        </property> \n' +\
                    '        <component name="sensor"> \n' + \
                    '            <property name="HDF5"> \n' + \
                    '                <value>' + listFiles[0] +'</value> \n' + \
                    '            </property> \n' + \
                    '            <property name="OUTPUT"> \n' + \
                    '                <value>' + listFiles[1] +'</value> \n' + \
                    '            </property> \n' + \
                    '        </component> \n' + \
                    '    </component> \n' + \
                    '</extractMetadata> \n'

        fp.write(strToSave)
    fp.close()
def main(argv):
    #argv is sensor dependent.
    #argv[0] = sensor name
    #argv[1:] = list of needed files. different for each sensor
    #argv[1:] for CSK : CSK CSKS2_RAW_B_HI_10_HH_RD_SF_20110214041210_20110214041218.h5 dummy.raw
    #argv[1:] for Alos : Alos IMG-BLA-BLA LED-BLA-BLA dummy.raw
    createInputFile(argv)
if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
