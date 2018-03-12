#! /usr/bin/env python3
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2013, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#


import sys
import os
class UrlUtils(object):

    def __init__(self,conf=''):
        try:
            os.environ['ARIA_DEBUG']
            sfx = '.debug'
        except:
            sfx = ''
        
            
        if conf:
            self._filename = os.path.join(os.environ['ARIAMH_HOME'],'conf',conf)
        else:  
            self._filename = os.path.join(os.environ['ARIAMH_HOME'],'conf/settings.conf'+ sfx)
        self._rest_url = None
        self._dav_url = None
        self._dav_u = None
        self._dav_p = None
        self._dem_url = None
        self._ned1_dem_url = None
        self._ned13_dem_url = None
        self._dem_u = None
        self._dem_p = None
        self._wbd_url = None
        self._wbd_u = None
        self._wbd_p = None
        self._grq_index_prefix = None
        self._version = None #db version 
        self._datasets_cfg = None
        self.readConf()
    
    def download(self,url,file,u=None,p=None):
        import os
        command = 'curl -k -u ' + (u if u else self.dav_u) + ':' + (p if p else self.dav_p) + ' -O ' + os.path.join(url,file)
        return os.system(command)

        
    
    @property 
    def dav_url(self):
        return self._dav_url
    @property 
    def dav_u(self):
        return self._dav_u
    @property 
    def dav_p(self):
        return self._dav_p
    @property 
    def dem_url(self):
        return self._dem_url
    @property 
    def ned1_dem_url(self):
        return self._ned1_dem_url
    @property 
    def ned13_dem_url(self):
        return self._ned13_dem_url
    @property 
    def dem_u(self):
        return self._dem_u
    @property 
    def dem_p(self):
        return self._dem_p
    @property 
    def wbd_url(self):
        return self._wbd_url
    @property 
    def wbd_u(self):
        return self._wbd_u
    @property 
    def wbd_p(self):
        return self._wbd_p
    @property 
    def rest_url(self):
        return self._rest_url
    @property 
    def grq_index_prefix(self):
        return self._grq_index_prefix
    @property 
    def version(self):
        return self._version
    @property 
    def datasets_cfg(self):
        return self._datasets_cfg
    
    def readConf(self):
        fp = open(self._filename)
        allL = fp.readlines()
        dc = {}
        for line in allL:
            ls = line.split('=')
            if(len(ls) == 2):                
                dc[ls[0]] = ls[1]
        fp.close()
        try:
            self._rest_url = dc['GRQ_URL'].strip()
        except:
            pass
        try:
            self._dav_url = dc['ARIA_DAV_URL'].strip()
        except:
            pass
        try:
            self._dav_u = dc['ARIA_DAV_U'].strip()
        except:
            pass
        try:
            self._dav_p = dc['ARIA_DAV_P'].strip()
        except:
            pass
        try:
            self._dem_url = dc['ARIA_DEM_URL'].strip()
        except:
            pass
        try:
            self._ned1_dem_url = dc['ARIA_NED1_DEM_URL'].strip()
        except:
            pass
        try:
            self._ned13_dem_url = dc['ARIA_NED13_DEM_URL'].strip()
        except:
            pass
        try:
            self._dem_u = dc['ARIA_DEM_U'].strip()
        except:
            pass
        try:
            self._dem_p = dc['ARIA_DEM_P'].strip()
        except:
            pass
        try:
            self._wbd_url = dc['ARIA_WBD_URL'].strip()
        except:
            pass
        try:
            self._wbd_u = dc['ARIA_WBD_U'].strip()
        except:
            pass
        try:
            self._wbd_p = dc['ARIA_WBD_P'].strip()
        except:
            pass
        try:
            self._grq_index_prefix = dc['GRQ_INDEX_PREFIX'].strip()
        except:
            pass
        try:
            self._version = dc['ARIA_DB_VERSION'].strip()
        except:
            pass
        try:
            self._datasets_cfg = dc['DATASETS_CONFIG'].strip()
        except:
            pass


def main():
    uu = UrlUtils()
    print(uu.rest_url,uu.dav_url,uu.dav_u,uu.dav_p,uu.version)
if __name__ == '__main__':
    sys.exit(main())
