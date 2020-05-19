#!/usr/bin/env python3

from __future__ import division
from builtins import str
from builtins import range
from builtins import object
from past.utils import old_div
import numpy as np
import os
import isce
import argparse
import h5py
import datetime
import pyproj
import pdb
import logging
import shutil
from time import time
from functools import wraps
from joblib import Parallel, delayed, dump, load
import isce_functions_alos2
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

@simple_time_tracker(_log)
def loadMetadata(inps):
    '''
    Load metadata for master and slave.
    '''


    ref_track = isce_functions_alos2.get_alos2_obj(inps.master)
    ref_frame_data = isce_functions_alos2.getTrackFrameData(ref_track) 
    #print(ref_frame_data)

    sec_track = isce_functions_alos2.get_alos2_obj(inps.slave)
    sec_frame_data = isce_functions_alos2.getTrackFrameData(sec_track)
    #print(sec_frame_data)

    inps.masterSwaths = ref_frame_data['swaths']
    inps.slaveSwaths = sec_frame_data['swaths']

    inps.sensingStart = min(ref_frame_data['sensingStartList'])
    inps.sensingStop = max(ref_frame_data['sensingEndList'])
    inps.midtime = inps.sensingStart + 0.5 * (
        inps.sensingStop - inps.sensingStart)
    inps.midnight = inps.sensingStart.replace(
        hour=0, minute=0, second=0, microsecond=0)

    inps.slaveSensingStart = min(ref_frame_data['sensingStartList'])
    inps.slaveSensingStop = max(ref_frame_data['sensingEndList'])
    inps.midtime = inps.slaveSensingStart + 0.5 * (
        inps.slaveSensingStop - inps.slaveSensingStart)
    inps.midnight = inps.slaveSensingStart.replace(
        hour=0, minute=0, second=0, microsecond=0)

    inps.nearRange = min(ref_frame_data['startingRangeList'])
    inps.farRange = max(ref_frame_data['endingRangeList'])
    
    print(inps)
    return inps

@simple_time_tracker(_log)
def generateSummary(inps):
    '''
    Add simple stats like swath range, height etc.
    '''
    refelp = Planet(pname='Earth').ellipsoid

if __name__ == '__main__':
    '''
    Main driver.
    '''

    #Command line parser
    inps = cmdLineParse()
    print(inps)
    
    #Load Metadata
    loadMetadata(inps)

    #Add summary information
    generateSummary(inps)
