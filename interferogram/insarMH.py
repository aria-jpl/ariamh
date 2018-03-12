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

class InsarMH(Insar):
    
    def __init__(self,cmdline=None):
        self._stage = None
        self._peg = None
        super(InsarMH,self).__init__(cmdline=cmdline)
        #Do not add the unwrapper. Use what we have 
        #super(InsarMH,self)._add_methods()

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
            super(InsarMH,self)._configure()

    @property
    def unwrapList(self):
        return self._unwrapList
   
    @unwrapList.setter
    def unwrapList(self,val):
        self._unwrapList = val
    
    '''
    Keep it as an exmaple on how to overwrite a run method
    def _add_methods(self):
        from interferogram.runUnwrapSnaphu import runUnwrap
        import types
        # overwrite the runUnwrapper method 
        self.runUnwrapper = types.MethodType(runUnwrap,self)
    '''
    ## Default pickle behavior
    def __getstate__(self):
        d = dict(self.__dict__)
        del d['runUnwrapper']
        return d

    ## Default unpickle behavior
    def __setstate__(self, d):
        self.__dict__.update(d)
        self._add_methods()
        return None

