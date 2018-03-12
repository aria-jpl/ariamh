#!/usr/bin/env python

import numpy as np
from datetime import datetime as DT
import requests
import ast
import sys
import pyproj
from collections import OrderedDict

errorCodes ={ 
              'GPS data unavailable:', 10,
              'Not enough stations: ', 20
            }

gpsSources = ['measures_comb', 'jpl_ats']

WGS84llh = pyproj.Proj(proj='latlong', ellps='WGS84', datum='WGS84')
WGS84xyz = pyproj.Proj(proj='geocent', ellps='WGS84', datum='WGS84')


class GPS(object):
    '''
    Class to store information about one GPS station on one day.
    '''
    def __init__(self, linestr,formatstr, source=None):
        '''
        Create the GPS station object using the line string and format string.
        '''

        fields = formatstr.split(';')
        values = linestr.split(';')
        self.source = source
        if len(fields) != len(values):
            raise Exception(" Length of the format and value string dont match")

        for (key, value) in zip(fields, values):
            if key:
                key1 = key.encode('ascii', 'ignore')
                val1 = value.encode('ascii', 'ignore')
                try:
                    val1 = ast.literal_eval(val1)
                except:
                    pass

                setattr(self, key1.split()[0], val1)

        self.localTransform = None
        self.refError = None
        return

    def verify(self):
        '''
        Verify if the XYZ to LLH data in the results are consistent.
        '''

        res = pyproj.transform(WGS84xyz, WGS84llh, self.x, self.y, self.z)

        print 'Listed: ', self.wgsLon, self.wgsLat, self.wgsHt
        print 'Estimated: ', res[0], res[1], res[2]
        print 'Error: ', self.wgsLon - res[0], self.wgsLat-res[1], self.wgsHt - res[2]

    def setupLocalCoordinates(self):
        '''
        Sets up the local coordinate system around given point.
        Adopted from metpy toolbox.
        '''

        ###Local center in XYZ
        localCenter = np.array([self.x, self.y, self.z])
        llh = np.array(pyproj.transform(WGS84xyz, WGS84llh, self.x, self.y, self.z))    

        #XYZ coords of point above center
        aboveCenter = np.array(pyproj.transform(WGS84llh, WGS84xyz, llh[0], llh[1], llh[2]+1.0))

        n = aboveCenter - localCenter
        n = n/np.linalg.norm(n)
        localz = n

        #Unit vector projection
        d = np.dot(n, aboveCenter)
        P = np.identity(3) - np.transpose(np.vstack((n,n,n))) *np.vstack((n,n,n))

        ###Pick a point directly to the north of the reference location
        northCenterECEF = np.array(pyproj.transform(WGS84llh, WGS84xyz, llh[0], llh[1]+0.0001, llh[2]))
        localy = np.dot(P, northCenterECEF)
        localy = -localy / np.linalg.norm(localy) # negation gets x and y pointing in the right direction
        
        #local x is y (cross) z to get an orthogonal system
        localx = np.transpose(np.cross(localy, localz))
        localx = localx / np.linalg.norm(localx)
        
        
        Tmat = np.zeros((3,3))
        Tmat[0,:] = localx
        Tmat[1,:] = localy
        Tmat[2,:] = localz
        #
        self.localTransform = Tmat

        ####Approximate error in reference position
        self.refError = np.sqrt(np.diag(np.dot(self.localTransform, np.dot(np.diag([self.x_sig**2, self.y_sig**2, self.z_sig**2]), self.localTransform.T))))

        return
        
    def toENU(self, inp):
        '''
        Gives the input vector to point inp from the current point.
        '''

        if self.localTransform is None:
            self.setupLocalCoordinates()

        localCenter = np.array([self.x, self.y, self.z])
        inPoint = np.array([inp.x, inp.y, inp.z])
        res = np.dot(self.localTransform, inPoint-localCenter)

        ####Approximate error transform
        ####Assuming diag covariance to diag covariance

        differr = np.array([inp.x_sig**2 + self.x_sig**2 , inp.y_sig**2 + self.y_sig**2, inp.z_sig**2 + self.z_sig**2])
        err = np.dot(self.localTransform, np.diag(differr))
        err = np.sqrt(np.diag(np.dot(err, self.localTransform.T)))
        return res,err

    def __str__(self):
        '''
        Print the given station information.
        '''

        import pprint 
        return pprint.pformat(self.__dict__, indent=4)



def getGPSinBox(yyyymmdd, snwe, source='jpl_ats'):
    '''
    Download GPS data corresponding to given date from UCSD web service.
    '''

    if source.lower() not in gpsSources:
        raise Exception('Unknown GPS data source: %s'%(source))

    base_url = 'http://geoapp02.ucsd.edu:8080/gpseDB/coord?op=getXYZ'
    if isinstance(yyyymmdd,str):
        datestr = DT.strptime(yyyymmdd,'%Y%m%d').strftime('%Y-%m-%d')
    else:
        datestr = yyyymmdd.strftime('%Y-%m-%d')

    fields={}
    fields['date']=datestr
    fields['fil'] = 'flt'
    fields['minLat'] = snwe[0]
    fields['maxLat'] = snwe[1]
    fields['minLon'] = snwe[2]
    if fields['minLon'] < 0:
        fields['minLon'] += 360.0

    fields['maxLon'] = snwe[3]
    if fields['maxLon'] < 0:
        fields['maxLon'] += 360.0

    fields['source'] = source

    final_url=''+base_url
    for key,val in fields.iteritems():
        final_url += '&{0}={1}'.format(key,val)

    req = requests.get(final_url, verify=False)
    req.raise_for_status()
    strings = req.text.split('\n')
    sourcestr = strings[0]
    formatstr = strings[1]

    gpsStns = {}
    for string in strings[2:-1]:
        newStn = GPS(string, formatstr, source=sourcestr)
        gpsStns[newStn.site] = newStn

    return gpsStns

class GPSstn(object):
    '''
    Object to hold GPS data corresponding to one station.
    '''
    def __init__(self, name, lat, lon, ii, jj):
        '''
        Constructor with the station name.
        '''

        self.name = name
        self.lat = lat
        self.lon = lon
        self.ii = ii
        self.jj = jj
        self.dates = OrderedDict()

    def addObservation(self, date, enu, err):
        '''
        Add a GPS observation.
        '''
        self.dates[date] = [enu, err]

    def getENU(self, date):
        '''
        Return ENU coordinates.
        '''
        try:
            pos = self.dates[date][0]
        except:
            print 'No GPS data for day %s and station %s'%(date, self.name)
            sys.exit(errorCodes['GPS Data Error'])

        return pos

    def getError(self, date):
        '''
        Return ENU Error.
        '''
        try:
            pos = self.dates[date][1]
        except:
            print 'No GPS data for day %s and station %s'%(date, self.name)
            sys.exit(errorCodes['GPS Data Error'])

        return pos

    def getPosition(self):
        '''
        Return ii, jj.
        '''
        return (self.ii, self.jj)

    def getLatLon(self):
        '''
        Return lat, lon.
        '''
        return (self.lat, self.lon)

    def dump_sopac_file(self, fname):
        '''
        Create a SOPAC style GPS file without model header.
        '''
        with open(fname, 'w') as fid:
            dateorder = np.sort(self.dates.keys())
            for date in dateorder:
                dateObj = DT.strptime(date, '%Y%m%d')
                dayYear = dateObj.timetuple().tm_yday
                fracYear = dateObj.year + (dayYear / 365.25)

                vals = self.dates[date][0]
                outstr = '{0:4.4f} {1:4d} {2:03d} {3:3.4f} {4:3.4f} {5:3.4f} {6:3.4f} {7:3.4f} {8:3.4f}\n'.format(fracYear, dateObj.year, dayYear, vals[1]/1000.0, vals[0]/1000.0, vals[2]/1000.0, 0.002, 0.002, 0.002)
                fid.write(outstr)

        return



    def __str__(self):
        '''
        Conversion to string.
        '''
        ostr = 'Site : ' + self.name + '\n' +\
                'Lat: %f  Lon: %f \n'%(self.lat, self.lon) + \
                'Row: %d  Col: %d \n'%(self.ii, self.jj)

        for date in self.dates.keys():
            ostr += date +"   " +  str(self.getENU(date)) + "   " + str(self.getError(date)) + '\n'

        return ostr


if __name__ == '__main__':
    '''
    Test driver.
    '''
    
    stns = getGPSinBox('20110101',[34.0,35.0,242.0,240.0])
    stn = stns['alpp']

    print 'Reference: ', stn
    print 'Point :', stns['ana1']

    print stn.toENU(stns['ana1'])
