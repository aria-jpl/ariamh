#!/usr/bin/env python3
import os, sys, requests, json
from pprint import pprint
from utils.UrlUtils import UrlUtils
from utils.queryBuilder import buildQuery,postQuery
import argparse

def getMetadata(track=None, frame=None, beam=None, passdir=None,platform=None):
    """
    Download metadata json from product repo for product with ID passed in.
    """
    uu = UrlUtils()

    params = {
    "dataset": "interferogram",
    "trackNumber": str(track),
    "direction": passdir,
    "latitudeIndexMin": frame[0],
    "latitudeIndexMax": frame[1],
    "beamID" : beam,
    'system_version':uu.version
    }

   
    # get GRQ request
    '''
    r = requests.get(url, params=params, verify=False)
    r.raise_for_status()
    res_json = r.json()
    if res_json['count'] == 0:
        raise ValueError("Found no interferogram product for Track %d, Frame %d."%(track,frame))
    '''
    query = buildQuery(params,'within')
    metList,status = postQuery(query)

    return metList

def checkBbox(res_json):
    '''
    Ensure that all the results have the same bbox.
    '''
    
    uBox = []
    indBox = []
    countUBox = []

    num = res_json['count']
    print('Initial number of returned results: ', num)

    ######Initialize unique list
#    bounds = res_json['result'][0]['imageCorners']
#    bbox = [bounds['maxLat'], bounds['minLat'],bounds['maxLon'],bounds['minLon']]
    bbox = res_json['result'][0]['refbbox']
    uBox.append(bbox)
    countUBox.append(1)
    indBox.append(0)

    for resnum in range(1,num):
#        bounds = res_json['result'][resnum]['imageCorners']
#        box = [bounds['maxLat'], bounds['minLat'], bounds['maxLon'], bounds['minLon']]
        box = res_json['result'][resnum]['refbbox']
        found = False

        for index,elem in enumerate(uBox):
            if found:
                continue

            if box==elem:
                indBox.append(index)
                countUBox[index] += 1
                found = True

        if not found:
#            print res_json['result'][resnum]
            uBox.append(box)
            countUBox.append(1)
            indBox.append(len(uBox)-1)

    print('Number of unique boxes: ', len(uBox))
    print('Counts for unique boxes: ', countUBox)
    if len(uBox) > 1:
        print('Warning: More than one set of geocoded results found.')
        print('Picking the largest returned group.')

        maxIndex = countUBox.index(max(countUBox))

        result = {}
        result['count'] = countUBox[maxIndex]
        result['message'] = ""
        result['result'] = []

        for index,elem in enumerate(res_json['result']):
            if indBox[index] == maxIndex:
                result['result'].append(elem)

        return result
    else:
        print('All returned results belong to single group.')
        return res_json


def parseCmdLine():
    '''
    Command Line Parser.
    '''
    parser = argparse.ArgumentParser(description='Gets the meta data associated with an interferogram and dumps it as a json file.')
    parser.add_argument('--track', dest='track', type=int, required=True,
            help = 'Track number.')
    parser.add_argument('--frame', dest='frame', type=int, nargs=2, required=True,
            help = 'Frame number.')
    parser.add_argument('--beam', dest='beam', type=str, default=None,
            help = 'Beam Id.')
    parser.add_argument('--pass', dest='passdir', type=str, required=True, 
            choices = ('asc','dsc'), help='Pass direction')
    parser.add_argument('--out', dest='out', type=str, default='metadata.json',
            help = 'Output file.')
    parser.add_argument('--platform', dest='platform', type=str, default='CSK',
            help = 'Output file.')
    return parser.parse_args()

if __name__ == "__main__":
    #inps = parseCmdLine()
    inps = json.load(open(sys.argv[1]))
    track = inps['trackNumber']
    frame = [inps['latitudeIndexMin'],inps['latitudeIndexMax']]
    beam = inps['beamID']
    passdir = inps['direction']
    platform = inps['platform']

    res = getMetadata(track=track, frame=frame, beam=beam, passdir=passdir,platform=platform)
    #search already provide correct bbox
    #res = checkBbox(res)
    with open(inps['metaFile'], 'w') as fid:
        json.dump(res, fid, indent=2, sort_keys=True)
