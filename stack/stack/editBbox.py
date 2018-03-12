#!/usr/bin/env python

import os
import sys
import stackSetup as SS
import cPickle
import argparse
import numpy 

def edit_bbox(flist, snwe=()):
    """
    view the topo.snwe tuple in each of the PICKLE/unwrap
    """
    if not snwe:
        minLat = minLon = 360.
        maxLat = maxLon = -360.

        plist = []
        count = 0
        for x in flist:
            F = open(x, 'rb')
            p = cPickle.load(F)
            snwe = p.topo.snwe
            count+=1
            print 'Parsing: ', count, x, snwe
            minLat = min(minLat, snwe[0])
            maxLat = max(maxLat, snwe[1])
            minLon = min(minLon, snwe[2])
            maxLon = max(maxLon, snwe[3])
            F.close()
#too much memory usage if > 19       plist.append(p)

        snwe = (minLat, maxLat, minLon, maxLon)
        print "inclusive snwe = ", snwe

    else:
        print "using input snwe = ", snwe

    update_bbox(flist, snwe)

    return snwe

def update_bbox(flist, snwe):
    '''Actually modify the pickle files in each of the dirs.'''
    count = 0
    for x in flist:
        count+=1
        print 'Updating: ', count, x
        F = open(x, 'rb')
        p = cPickle.load(F)
        p.topo.snwe = snwe
        F.close()
        F = open(x, 'wb')
        cPickle.dump(p, F)
        F.close()

    return snwe

def read_bbox_from_file(dirname='.', filename='bbox.snwe'):
    '''
    Read bbox from file. Return none if it does not exist.
    '''
    fname = os.path.join(dirname, filename)
    if os.path.exists(fname):
        fid = open(fname,'r')
        data = fid.read()
        fid.close()
        snwe = [float(line) for line in data.split('\n')[0:4]]
        return tuple(snwe)
    else:
        return None

def write_bbox_to_file(snwe, dirname='.', filename='bbox.snwe'):
    '''
    Get the bbox from file. Returns none if it doesnt exist.
    '''
    fname = os.path.join(dirname, filename)
    if os.path.exists(fname):
        print 'Overwriting existing bbox.'

    fid = open(fname, 'w')
    for kk in snwe:
        fid.write("{0}\n".format(kk))

    fid.close()
    return

def parse():
    '''Command line parsing.'''
    parser = argparse.ArgumentParser(description='Edit bounding box for geocoding.')
    parser.add_argument('-d', action='store', default='.', dest='intdir', 
            help='Directory with IFGs as subdirectories', type=str)
    parser.add_argument('-ilist', action='store', default='', dest='ilist',
            help='List of interferograms to edit', type=str)
    parser.add_argument('-f', action='store_true', default=False, dest='force',
            help = 'Force recomputing and updating of bbox')

    inps = parser.parse_args()

    return inps

if __name__ == '__main__':

    inps = parse()

    snwe = ()

    currSnwe = read_bbox_from_file(dirname=inps.intdir)

    if inps.ilist in [None,'']:
        pairs = SS.getPairDirs(inps.intdir)
    else:
        pairs = SS.pairDirs_from_file(inps.ilist, base=inps.intdir)

    flist = [os.path.join(val, 'PICKLE/unwrap') for val in pairs]

    if (currSnwe is None) or inps.force:
        newSnwe = edit_bbox(flist)
        write_bbox_to_file(newSnwe, dirname=inps.intdir)
    else:
        print 'Using predefined: ', currSnwe
        newSnwe = edit_bbox(flist, snwe=currSnwe)

    print 'Final Snwe: ', newSnwe
