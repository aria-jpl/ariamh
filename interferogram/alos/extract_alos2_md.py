#!/usr/bin/env python3
import glob
import os
from subprocess import check_call, check_output
import pickle
import argparse
import datetime
import json
import re
import requests

def create_alos2app_xml(dir_name):
    fp = open('alos2App.xml', 'w')
    fp.write('<alos2App>\n')
    fp.write('    <component name="alos2insar">\n')
    fp.write('        <property name="master directory">{}</property>\n'.format(os.path.abspath(dir_name)))
    fp.write('        <property name="slave directory">{}</property>\n'.format(os.path.abspath(dir_name)))
    fp.write('    </component>\n')
    fp.write('</alos2App>\n')
    fp.close()


def loadProduct(xmlname):
    '''
    Load the product using Product Manager.
    '''
    # from Cunren's code on extracting track data from alos2App
    import isce, isceobj
    from iscesys.Component.ProductManager import ProductManager as PM
    pm = PM()
    pm.configure()
    obj = pm.loadProduct(xmlname)
    return obj


def loadTrack(date):
    '''
    date: YYMMDD
    '''
    # from Cunren's code on extracting track data from alos2App
    track = loadProduct('{}.track.xml'.format(date))
    track.frames = []
    frameParameterFiles = sorted(glob.glob(os.path.join('f*_*', '{}.frame.xml'.format(date))))
    for x in frameParameterFiles:
        track.frames.append(loadProduct(x))
    return track

def getMetadataFromISCE(track):
    # from Cunren's code on extracting track data from alos2App
    import isce, isceobj
    from isceobj.Alos2Proc.Alos2ProcPublic import getBboxRdr

    #####################################
    # in image coordinate
    #         1      2
    #         --------
    #         |      |
    #         |      |
    #         |      |
    #         --------
    #         3      4
    # in geography coorindate
    #        1       2
    #         --------
    #         \       \
    #          \       \
    #           \       \
    #            --------
    #            3       4
    #####################################

    pointingDirection = {'right': -1, 'left': 1}
    bboxRdr = getBboxRdr(track)
    rangeMin = bboxRdr[0]
    rangeMax = bboxRdr[1]
    azimuthTimeMin = bboxRdr[2]
    azimuthTimeMax = bboxRdr[3]

    # in image coordinate
    # corner 1
    llh1 = track.orbit.rdr2geo(azimuthTimeMin, rangeMin, height=0, side=pointingDirection[track.pointingDirection])
    # corner 2
    llh2 = track.orbit.rdr2geo(azimuthTimeMin, rangeMax, height=0, side=pointingDirection[track.pointingDirection])
    # corner 3
    llh3 = track.orbit.rdr2geo(azimuthTimeMax, rangeMin, height=0, side=pointingDirection[track.pointingDirection])
    # corner 4
    llh4 = track.orbit.rdr2geo(azimuthTimeMax, rangeMax, height=0, side=pointingDirection[track.pointingDirection])

    # re-sort in geography coordinate
    if track.passDirection.lower() == 'descending':
        if track.pointingDirection.lower() == 'right':
            footprint = [llh2, llh1, llh4, llh3]
        else:
            footprint = [llh1, llh2, llh3, llh4]
    else:
        if track.pointingDirection.lower() == 'right':
            footprint = [llh4, llh3, llh2, llh1]
        else:
            footprint = [llh3, llh4, llh1, llh2]

    # footprint
    return footprint, azimuthTimeMin, azimuthTimeMax


def get_alos2_obj(dir_name):
    track = None
    img_file = sorted(glob.glob(os.path.join(dir_name, 'IMG*')))

    if len(img_file) > 0:
        match = re.search('IMG-[A-Z]{2}-(ALOS2)(.{05})(.{04})-(\d{6})-.{4}.*',img_file[0])
        if match:
            date = match.group(4)
            create_alos2app_xml(dir_name)
            check_output("alos2App.py --steps --end=preprocess", shell=True)
            track = loadTrack(date)
            track.spacecraftName = match.group(1)
            track.orbitNumber = match.group(2)
            track.frameNumber = match.group(3)

    return track


def create_alos2_md_json(dirname):
    track = get_alos2_obj(dirname)

    bbox, sensingStart, sensingEnd = getMetadataFromISCE(track)
    md = {}
    md['geometry'] = {
        "coordinates":[[
        bbox[0][1:None:-1], # NorthWest Corner
        bbox[1][1:None:-1], # NorthEast Corner
        bbox[3][1:None:-1], # SouthWest Corner
        bbox[2][1:None:-1], # SouthEast Corner
        bbox[0][1:None:-1],
        ]],
        "type":"Polygon"
    }
    md['start_time'] = sensingStart.strftime("%Y-%m-%dT%H:%M:%S.%f")
    md['stop_time'] = sensingEnd.strftime("%Y-%m-%dT%H:%M:%S.%f")
    md['absolute_orbit'] = track.orbitNumber
    md['frame'] = track.frameNumber
    md['flight_direction'] = 'asc' if 'asc' in track.catalog['passdirection'] else 'dsc'
    md['satellite_name'] = track.spacecraftName
    md['source'] = "isce_preprocessing"

    return md

def create_alos2_md_isce(dirname, filename):
    md = create_alos2_md_isce(dirname)
    with open(filename, "w") as f:
        json.dump(md, f, indent=2)
        f.close()

def cmdLineParse():
    '''
    Command line parser.
    '''
    parser = argparse.ArgumentParser( description='extract metadata from ALOS2 1.1 with ISCE')
    parser.add_argument('--dir', dest='alos2dir', type=str, default=".",
            help = 'directory containing the L1.1 ALOS2 CEOS files')
    parser.add_argument('--output', dest='op_json', type=str, default="alos2_md.json",
                        help='json file name to output metadata to')
    return parser.parse_args()

if __name__ == '__main__':
    args = cmdLineParse()
    insar_obj = get_alos2_obj(args.alos2dir)
    create_alos2_md_isce(insar_obj, args.op_json)






