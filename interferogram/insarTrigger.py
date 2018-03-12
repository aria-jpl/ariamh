#! /usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
from __future__ import print_function
from iscesys.Compatibility import Compatibility
Compatibility.checkPythonVersion()
from insarApp import Insar
import os
import time
import isceobj
import logging
import logging.config
logging.config.fileConfig(
    os.path.join(os.environ['ISCE_HOME'], 'defaults', 'logging',
        'logging.conf')
)

logger = logging.getLogger('isce.insar') 

class InsarTrigger(Insar):
    
    def __init__(self,cmdline=None):
        self._peg = None
        super(InsarTrigger,self).__init__(cmdline=cmdline)
    @property
    def peg(self):
        return self._peg
    @peg.setter
    def peg(self,val):
        self._peg = val
    @property
    def insar(self):
        return self._insar
    @insar.setter
    def insar(self,val):
        self._insar = val
    
    def _configure(self):
        if hasattr(self._insar,'geocode_bbox'):
            self.geocode_bbox = self._insar.geocode_bbox
            super(InsarTrigger,self)._configure()

  

