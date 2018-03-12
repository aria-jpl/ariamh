#! /usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


from iscesys.Compatibility import Compatibility
Compatibility.checkPythonVersion()

import os
import logging
import logging.config
logging.config.fileConfig(
    os.path.join(os.environ['ISCE_HOME'], 'defaults', 'logging',
        'logging.conf')
)
logger = logging.getLogger('isce.insar')
import isce
from FrameInfoExtractor import FrameInfoExtractor as FIE
from utils.contextUtils import toContext

from iscesys.Component.Application import Application

class ExtractMetadata(Application):
    """Insar Application class:

    Implements InSAR processing flow for a pair of scenes from
    sensor raw data to geocoded, flattened interferograms.
    """
    

    def _parameters(self):
        """
        Define the user configurable parameters for this application
        """
        self._metadataFile = self.parameter('metadataFile', public_name='metadata file', default="metadata.json",
                                         type=str, mandatory=True,
                                         doc="Filenane where to dump the metadata intoa  json format")
        self._sensorName = self.parameter('sensorName', public_name='sensor name', default=None,
                                         type=str, mandatory=True,
                                         doc="Sensor name")
        
        self._dopplerMethod = self.parameter('dopplerMethod', public_name='doppler method',default='useDOPIQ',
                                            type=str, mandatory=False,
                                            doc="Doppler calculation method.Choices: "+
                                                "'useDOPIQ', 'useCalcDop', 'useDoppler'.")
        self._datasetType = self.parameter('datasetType', public_name='datasetType', default=None,
                                         type=str, mandatory=True,
                                         doc="The dataset type, such as raw, slc.")
        
        
        return

    def _facilities(self):
        """
        Define the user configurable facilities for this application.
        """

        self.sensor = self.facility('sensor', public_name='Sensor',
                                    module='isceobj.Sensor', factory='createSensor',
                                    args=(self.sensorName,), mandatory=True,
                                    doc="Raw data object")


        self.doppler = self.facility('doppler', public_name='Doppler',
                                       module='isceobj.Doppler', factory='createDoppler',
                                       args=(self.dopplerMethod,), mandatory=False,
                                       doc="Doppler calculation method")


        return

    def _init(self):
        return
    
    def _configure(self):
        return

    def _finalize(self):
        return

    def help(self):
        from isceobj.Sensor import SENSORS
        print(self.__doc__)
        print("The currently supported sensors are: ", list(SENSORS.keys()))
        return None
    
    def main(self):
        
        self.help()
        import time
        import math
        import isceobj
        from make_raw import make_raw
        
        timeStart = time.time()


        exit = 0
        process = 'extractMetadata'
        message = 'Info extraction succeeded'
        
        try: 
            makeRaw = make_raw()
            makeRaw.wireInputPort(name='sensor', object=self.sensor)
            makeRaw.wireInputPort(name='doppler', object=self.doppler)
            makeRaw.make_raw()
        except Exception as e:
            exit = 1
            message = 'make_raw failed with exception ' + str(e)
            toContext(process,exit,message)
            raise Exception
        try:
            self.frame = makeRaw.getFrame()
            self.frame._squintAngle = math.radians(makeRaw.getSquint())
            self.frame.doppler = makeRaw.dopplerValues.getDopplerCoefficients()[0]
            self.frame.datasetType = self.datasetType 
            fie = FIE()
            frameInfo = fie.extractInfoFromFrame(self.frame)
        except Exception as e:
            exit = 2
            message = 'extractInfoFromFrame failed with exception ' + str(e)
            toContext(process,exit,message)
            raise Exception
        
        try:
            if(frameInfo):
                frameInfo.dump(self.metadataFile)
                dummyFile = self.frame.getImage().getFilename()
                os.system("rm -rf " + dummyFile  + "*")
        except Exception as e:
            exit = 3
            message = 'saving metadata file failed with exception ' + str(e)
            toContext(process,exit,message)
            raise Exception
            
        #if it gets here return 0
        toContext(process,exit,message)
        return 0
    @property
    def datasetType(self):
        return self._datasetType

    @datasetType.setter
    def datasetType(self,val):
        self._datasetType = val

    @property
    def frame(self):
        return self._frame

    @frame.setter
    def frame(self,val):
        self._frame = val
    @property
    def dopplerMethod(self):
        return self._dopplerMethod

    @dopplerMethod.setter
    def dopplerMethod(self,val):
        self._dopplerMethod = val
  
    @property
    def sensorName(self):
        return self._sensorName

    @sensorName.setter
    def sensorName(self,val):
        self._sensorName = val
  
    @property
    def sensor(self):
        return self._sensor

    @sensor.setter
    def sensor(self,val):
        self._sensor = val
  
    @property
    def doppler(self):
        return self._doppler

    @doppler.setter
    def doppler(self,val):
        self._doppler = val
  
    @property
    def metadataFile(self):
        return self._metadataFile

    @metadataFile.setter
    def metadataFile(self,val):
        self._metadataFile = val
  
    def __init__(self):
        import isceobj.InsarProc as InsarProc
        from iscesys.StdOEL.StdOELPy import StdOEL as ST
        self._frame = None
        self._sensorName = None
        self._metadataFile = None
        self._dopplerMethod = None
        self._sensor = None
        self._doppler = None
        self._stdWriter = ST()
        self._stdWriter.createWriters()
        self._stdWriter.configWriter("log","",True,"insar.log")
        self._stdWriter.init()
        Application.__init__(self,"extractMetadata")

 
        return


if __name__ == "__main__":
    extractor = ExtractMetadata()
    extractor.configure()
    print(extractor.run())
    
