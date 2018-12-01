#!/usr/bin/env python3

import numpy as np
import os
import isce
import argparse
import h5py
import datetime
import pyproj
import pdb
import logging
from time import time
from functools import wraps

from iscesys.Component.ProductManager import ProductManager as PM
from isceobj.Orbit.Orbit import Orbit
from isceobj.Planet.Planet import Planet

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('makeGeocube')


def simple_time_tracker(log_fun):

    def _simple_time_tracker(fn):

        @wraps(fn)
        def wrapped_fn(*args, **kwargs):
            start_time = time()

            try:
                result = fn(*args, **kwargs)
            finally:
                elapsed_time = time() - start_time

                # log the result
                log_fun({
                    'function_name': fn.__name__,
                    'total_time': elapsed_time,
                })

            return result

        return wrapped_fn

    return _simple_time_tracker


def _log(message):
    logger.info('[SimpleTimeTracker] {function_name} {total_time:.3f}'.format(
        **message))


def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser(description='Simulate metadata cube')

    parser.add_argument(
        '-m',
        '--master',
        dest='master',
        type=str,
        required=True,
        help='Folder with unpacked master data')
    parser.add_argument(
        '-s',
        '--slave',
        dest='slave',
        type=str,
        required=True,
        help='Folder with unpacked slave data')
    parser.add_argument(
        '-o',
        '--out',
        dest='outh5',
        type=str,
        required=True,
        help='Output HDF5 file')
    parser.add_argument(
        '-z',
        '--hgt',
        dest='heights',
        type=float,
        nargs='+',
        default=[-1500., 0., 3000., 9000.],
        help='height values')
    parser.add_argument(
        '-y',
        '--yspc',
        dest='yspacing',
        type=float,
        default=0.1,
        help='y spacing in geocoded space (see EPSG)')
    parser.add_argument(
        '-x',
        '--xspc',
        dest='xspacing',
        type=float,
        default=0.1,
        help='x spacing in geocoded space (see EPSG)')
    parser.add_argument(
        '-e',
        '--epsg',
        dest='epsg',
        type=int,
        default=4326,
        help='EPSG code for geocoded products')
    parser.add_argument(
        '-nodata',
        '--nodata',
        dest='nodata',
        type=float,
        default=-9999,
        help='No-data value, default is -9999')

    inps = parser.parse_args()
    inps.heights = np.sort(np.array(inps.heights))
    if 0. not in inps.heights:
        raise Exception('One of the heights has to be zero.')

    return inps


def loadProduct(xmlname):
    '''
    Load product using ISCE's product loader.
    '''

    pm = PM()
    pm.configure()

    obj = pm.loadProduct(xmlname)

    return obj


def getUTMZone(inps):
    '''
    Determine UTM zone for scene center. Can update to use majority of scene later.
    '''

    def latlon_to_zone_number(latitude, longitude):
        if 56 <= latitude < 64 and 3 <= longitude < 12:
            return 32

        if 72 <= latitude <= 84 and longitude >= 0:
            if longitude < 9:
                return 31
            elif longitude < 21:
                return 33
            elif longitude < 33:
                return 35
            elif longitude < 42:
                return 37

        return int((longitude + 180) / 6) + 1

    def latitude_to_zone_letter(latitude):
        ZONE_LETTERS = "CDEFGHJKLMNPQRSTUVWXX"
        if -80 <= latitude <= 84:
            return ZONE_LETTERS[int(latitude + 80) >> 3]
        else:
            return None

    lat = inps.sceneCenter[0]
    lon = inps.sceneCenter[1]

    zone = latlon_to_zone_number(lat, lon)
    inps.utmzone = str(zone) + latitude_to_zone_letter(lat)

    if lat < 0:
        pad = '+south'
    else:
        pad = ''

    inps.utm = '+proj=utm +zone={0} {1} +ellps=WGS84 +datum=WGS84 +units=m +no_defs'.format(
        zone, pad)
    return inps.utm


def getMergedOrbit(product):

    ###Create merged orbit
    orb = Orbit()
    orb.configure()

    burst = product[0].bursts[0]
    #Add first burst orbit to begin with
    for sv in burst.orbit:
        orb.addStateVector(sv)

    for pp in product:
        ##Add all state vectors
        for bb in pp.bursts:
            for sv in bb.orbit:
                if (sv.time < orb.minTime) or (sv.time > orb.maxTime):
                    orb.addStateVector(sv)

        return orb


@simple_time_tracker(_log)
def loadMetadata(inps):
    '''
    Load metadata for master and slave.
    '''

    masterSwaths = []
    slaveSwaths = []
    for ii in range(1, 4):
        mxmlname = os.path.join(inps.master, 'IW{0}.xml'.format(ii))
        sxmlname = os.path.join(inps.slave, 'IW{0}.xml'.format(ii))

        if os.path.exists(mxmlname) and os.path.exists(sxmlname):
            masterSwaths.append(loadProduct(mxmlname))
            slaveSwaths.append(loadProduct(sxmlname))

    inps.masterSwaths = masterSwaths
    inps.slaveSwaths = slaveSwaths

    inps.sensingStart = min([x.sensingStart for x in inps.masterSwaths])
    inps.sensingStop = max([x.sensingStop for x in inps.masterSwaths])
    inps.midtime = inps.sensingStart + 0.5 * (
        inps.sensingStop - inps.sensingStart)
    inps.midnight = inps.sensingStart.replace(
        hour=0, minute=0, second=0, microsecond=0)
    inps.slaveSensingStart = min([x.sensingStart for x in inps.slaveSwaths])
    inps.slaveMidnight = inps.slaveSensingStart.replace(
        hour=0, minute=0, second=0, microsecond=0)
    inps.nearRange = min([x.startingRange for x in inps.masterSwaths])
    inps.farRange = max([x.farRange for x in inps.masterSwaths])

    return


@simple_time_tracker(_log)
def generateSummary(inps):
    '''
    Add simple stats like swath range, height etc.
    '''
    refelp = Planet(pname='Earth').ellipsoid

    inps.orbit = getMergedOrbit(inps.masterSwaths)
    inps.slaveorbit = getMergedOrbit(inps.slaveSwaths)

    sv = inps.orbit.interpolateOrbit(inps.midtime, method='hermite')

    inps.llh = refelp.xyz_to_llh(sv.getPosition())
    inps.hdg = inps.orbit.getENUHeading(inps.midtime)

    refelp.setSCH(inps.llh[0], inps.llh[1], inps.hdg)
    sch, vsch = refelp.xyzdot_to_schdot(sv.getPosition(), sv.getVelocity())

    inps.vsch = vsch

    logger.info('Platform LLH: ', inps.llh)
    logger.info('Platform heading: ', inps.hdg)
    logger.info('Platform velocity (SCH): ', inps.vsch)


@simple_time_tracker(_log)
def estimateGridPoints(inps):
    '''
    Estimate start and end.
    '''

    #pdb.set_trace()
    inps.proj4 = 'EPSG:{0}'.format(inps.epsg)
    inps.proj = pyproj.Proj(init=inps.proj4)
    inps.ecef = pyproj.Proj(proj='geocent', ellps='WGS84', datum='WGS84')
    inps.lla = pyproj.Proj(proj='latlong', ellps='WGS84', datum='WGS84')

    inps.earlyNear = inps.orbit.rdr2geo(inps.sensingStart, inps.nearRange)
    inps.lateNear = inps.orbit.rdr2geo(inps.sensingStop, inps.nearRange)

    inps.earlyFar = inps.orbit.rdr2geo(inps.sensingStart, inps.farRange)
    inps.lateFar = inps.orbit.rdr2geo(inps.sensingStop, inps.farRange)

    inps.sceneCenter = inps.orbit.rdr2geo(
        inps.midtime, 0.5 * (inps.nearRange + inps.farRange))

    pts = []
    for x in [inps.earlyNear, inps.lateNear, inps.earlyFar, inps.lateFar]:
        pts.append(
            list(pyproj.transform(inps.lla, inps.proj, x[1], x[0], x[2])))

    pts = np.array(pts)

    inps.x0 = (int(np.min(pts[:, 0]) / inps.xspacing) - 2) * inps.xspacing
    inps.x1 = (int(np.max(pts[:, 0]) / inps.xspacing) + 3) * inps.xspacing
    inps.Nx = int(np.round((inps.x1 - inps.x0) / inps.xspacing)) + 1

    inps.y0 = (int(np.min(pts[:, 1]) / inps.yspacing) - 2) * inps.yspacing
    inps.y1 = (int(np.max(pts[:, 1]) / inps.yspacing) + 3) * inps.yspacing
    inps.Ny = int(np.round((inps.y1 - inps.y0) / inps.yspacing)) + 1
    inps.utmproj = pyproj.Proj(getUTMZone(inps))


@simple_time_tracker(_log)
def writeInputs(inps, fid):
    '''
    Just record inputs for debugging.
    '''

    grp = fid.create_group('inputs')

    grp.create_dataset(
        'sensingStart',
        shape=(1, 1),
        data=[inps.sensingStart.isoformat().encode('ascii', 'ignore')],
        dtype='S27')
    grp.create_dataset(
        'sensingStop',
        shape=(1, 1),
        data=[inps.sensingStop.isoformat().encode('ascii', 'ignore')],
        dtype='S27')
    grp.create_dataset(
        'midtime',
        shape=(1, 1),
        data=[inps.midtime.isoformat().encode('ascii', 'ignore')],
        dtype='S27')
    grp.create_dataset('yspacing', data=inps.yspacing)

    grp.create_dataset('nearRange', data=inps.nearRange)
    grp.create_dataset('farRange', data=inps.farRange)
    grp.create_dataset('xspacing', data=inps.xspacing)
    grp.create_dataset('heights', data=inps.heights)

    orb = grp.create_group('orbit')
    orb.create_dataset(
        'times',
        shape=(len(inps.orbit._stateVectors), 1),
        data=[x.getTime().isoformat().encode('ascii') for x in inps.orbit],
        dtype='S27')
    orb.create_dataset(
        'position', data=np.array([x.getPosition() for x in inps.orbit]))
    orb.create_dataset(
        'velocity', data=np.array([x.getVelocity() for x in inps.orbit]))
    grp.create_dataset(
        'projection', data=[inps.proj4.encode('utf-8')], dtype='S200')
    grp.create_dataset(
        'localutm', data=[inps.utm.encode('utf-8')], dtype='S200')


@simple_time_tracker(_log)
def writeSummary(inps, fid):
    '''
    Write summary for debugging.
    '''

    grp = fid.create_group('summary')

    grp.create_dataset('earlyNear', data=np.array(inps.earlyNear))
    grp.create_dataset('earlyFar', data=np.array(inps.earlyFar))
    grp.create_dataset('lateFar', data=np.array(inps.lateFar))
    grp.create_dataset('lateNear', data=np.array(inps.lateNear))

    grp.create_dataset('sceneCenter', data=np.array(inps.sceneCenter))
    grp.create_dataset('heading', data=inps.hdg)
    grp.create_dataset('vsch', data=inps.vsch)


@simple_time_tracker(_log)
def processCube(inps, fid, no_data=-9999):
    '''
    Start generating the cube.
    '''

    def nvector(llh):
        '''
        Return n-vector at a given target.
        '''

        clat = np.cos(np.radians(llh[1]))
        slat = np.sin(np.radians(llh[1]))
        clon = np.cos(np.radians(llh[0]))
        slon = np.sin(np.radians(llh[0]))

        return np.array([clat * clon, clat * slon, slat])

    logger.info('Output grid size: {0} x {1} x {2}'.format(
        len(inps.heights), inps.Ny, inps.Nx))

    cube = fid.create_group('cube')
    cube.create_dataset('x0', data=inps.x0)
    cube.create_dataset('x1', data=inps.x1)
    cube.create_dataset('y0', data=inps.y0)
    cube.create_dataset('y1', data=inps.y1)

    lookangle = np.ones(
        (len(inps.heights), inps.Ny, inps.Nx), dtype=np.float32) * no_data
    incangle = np.ones(
        (len(inps.heights), inps.Ny, inps.Nx), dtype=np.float32) * no_data
    azangle = np.ones(
        (len(inps.heights), inps.Ny, inps.Nx), dtype=np.float32) * no_data
    azimuthtime = np.zeros((len(inps.heights), inps.Ny, inps.Nx),
                           dtype=np.float64)
    slantrange = np.zeros(
        (len(inps.heights), inps.Ny, inps.Nx), dtype=np.float64) * no_data
    bpar = np.ones(
        (len(inps.heights), inps.Ny, inps.Nx), dtype=np.float32) * no_data
    bperp = np.ones(
        (len(inps.heights), inps.Ny, inps.Nx), dtype=np.float32) * no_data
    slavetime = np.zeros((len(inps.heights), inps.Ny, inps.Nx),
                         dtype=np.float64)
    slaverange = np.zeros((len(inps.heights), inps.Ny, inps.Nx),
                          dtype=np.float64)
    latvector = np.zeros((inps.Ny), dtype=np.float64)
    lonvector = np.zeros((inps.Nx), dtype=np.float64)

    for ii in range(inps.Ny):
        yval = inps.y1 - ii * inps.yspacing
        #satutm = np.array( pyproj.transform( lla, utm, satllh[0], satllh[1], satllh[2]))
        latvector[ii] = yval

        logger.info("Running ROW: " + str(ii) + " of " + str(inps.Ny))

        for jj in range(inps.Nx):
            xval = inps.x0 + jj * inps.xspacing
            if ii == 0:
                lonvector[jj] = xval

            for ind, hh in enumerate(inps.heights):
                targproj = pyproj.transform(inps.proj, inps.lla, xval, yval, hh)
                targ = [targproj[1], targproj[0], targproj[2]]
                targxyz = pyproj.transform(inps.proj, inps.ecef, xval, yval, hh)
                targutm = pyproj.transform(inps.proj, inps.utmproj, xval, yval,
                                           hh)
                targnorm = nvector(targ)

                try:
                    mtaz, mrng = inps.orbit.geo2rdr(targ)
                except:
                    mtaz = None
                    mrng = None

                if mrng is not None:

                    sv = inps.orbit.interpolateOrbit(mtaz, method='hermite')
                    satpos = np.array(sv.getPosition())
                    satvel = np.array(sv.getVelocity())
                    satllh = np.array(
                        pyproj.transform(inps.ecef, inps.lla, satpos[0],
                                         satpos[1], satpos[2]))
                    satutm = np.array(
                        pyproj.transform(inps.lla, inps.utmproj, satllh[0],
                                         satllh[1], satllh[2]))
                    satnorm = nvector(satllh)

                    azimuthtime[ind, ii, jj] = (
                        mtaz - inps.midnight).total_seconds()
                    slantrange[ind, ii, jj] = mrng

                    losvec = (targxyz - satpos) / mrng
                    losvec = losvec / np.linalg.norm(losvec)

                    staz = None
                    srng = None
                    try:
                        staz, srng = inps.slaveorbit.geo2rdr(targ)
                        slavesat = inps.slaveorbit.interpolateOrbit(
                            staz, method='hermite')
                        slavexyz = np.array(slavesat.getPosition())
                    except:
                        slavexyz = np.nan * np.ones(3)

                    if srng is not None:
                        direction = np.sign(
                            np.dot(np.cross(losvec, slavexyz - satpos), satvel))
                        baseline = np.linalg.norm(slavexyz - satpos)
                        bparval = np.dot(losvec, slavexyz - satpos)
                        bpar[ind, ii, jj] = bparval
                        bperp[ind, ii, jj] = direction * np.sqrt(
                            baseline * baseline - bparval * bparval)
                        slavetime[ind, ii, jj] = (
                            staz - inps.slaveMidnight).total_seconds()
                        slaverange[ind, ii, jj] = srng

                    lookangle[ind, ii, jj] = np.degrees(
                        np.arccos(np.dot(satnorm, -losvec)))
                    incangle[ind, ii, jj] = np.degrees(
                        np.arccos(np.dot(targnorm, -losvec)))
                    azangle[ind, ii, jj] = np.degrees(
                        np.arctan2(satutm[1] - targutm[1],
                                   satutm[0] - targutm[0]))

    cube.create_dataset('bparallel', data=bpar)
    cube.create_dataset('bperp', data=bperp)
    cube.create_dataset('lookangle', data=lookangle)
    cube.create_dataset('incangle', data=incangle)
    cube.create_dataset('azangle', data=azangle)
    cube.create_dataset('secondsofday', data=azimuthtime)
    cube.create_dataset('slantrange', data=slantrange)
    cube.create_dataset('slavetime', data=slavetime)
    cube.create_dataset('slaverange', data=slaverange)
    cube.create_dataset('yspacing', data=inps.yspacing)
    cube.create_dataset('xspacing', data=inps.xspacing)
    cube.create_dataset('heights', data=inps.heights)
    cube.create_dataset('lons', data=lonvector)
    cube.create_dataset('lats', data=latvector)
    cube.create_dataset('nodata', data=np.float(no_data))


if __name__ == '__main__':
    '''
    Main driver.
    '''

    #Command line parser
    inps = cmdLineParse()

    #Load Metadata
    loadMetadata(inps)

    #Add summary information
    generateSummary(inps)

    ##Get corners
    estimateGridPoints(inps)

    ####Check for existing HDF5 file
    if os.path.exists(inps.outh5):
        logger.info('{0} file already exists'.format(inps.outh5))
        raise Exception('Output file already exists')

    ###Create h5 file
    fid = h5py.File(inps.outh5)

    ###Record inputs
    writeInputs(inps, fid)

    ###Record summary
    writeSummary(inps, fid)

    ####Generate cube
    processCube(inps, fid, no_data=inps.nodata)

    ####Close file
    fid.close()
