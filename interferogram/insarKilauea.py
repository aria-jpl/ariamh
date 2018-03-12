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
from interferogram.insarMH import InsarMH
import os
import time
import isceobj
import logging
logging.config.fileConfig(
    os.path.join(os.environ['ISCE_HOME'], 'defaults', 'logging',
        'logging.conf')
)

logger = logging.getLogger('isce.insar') 

class InsarKilauea(InsarMH):
    
    def __init__(self,cmdline=None):      
        self._peg = None
        super(InsarKilauea,self).__init__(cmdline=cmdline)
        #the _add_method gets called in the super __init__
        super(InsarKilauea,self)._add_methods()
        self._add_methods()
    @property
    def peg(self):
        return self._peg
    @peg.setter
    def peg(self,val):
        self._peg = val
    @property
    def unwrapList(self):
        return self._unwrapList
    @unwrapList.setter
    def unwrapList(self,val):
        self._unwrapList = val
    
   
    def _add_methods(self):
        from interferogram.runUnwrapSnaphu import runUnwrap
        import types
        # overwrite the runUnwrapper method 
        self.runUnwrapper = types.MethodType(runUnwrap,self, self.__class__)
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


