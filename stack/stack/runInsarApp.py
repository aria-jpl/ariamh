#!/usr/bin/env python 

import os 
import numpy as np 
import sys
import argparse
import stackSetup as SS

def parse():
    parser = argparse.ArgumentParser(description='Process interferograms from raw to unwrap stage.')

    parser.add_argument('-d', action='store', default='.', 
        dest='dirname', help='Directory with prepared interferogram dirs.',
        type=str)
    parser.add_argument('-force', action='store_true', default=False,
        dest='force', help='Force reprocessing.')
    parser.add_argument('-l', action='store', default='',
        dest='subset', help='Consider only a subset of dirs given in file.',
        type=str)
    parser.add_argument('--start', action='store', default='startup',
            dest='start', help='Starting processing step.', type=str)
    parser.add_argument('--end', action='store', default='endup',
            dest='end', help='Last processing step.', type=str)
    parser.add_argument('--noclean', action='store_true', default=False,
            dest='noclean', help='Clean up raw and slc files.')

    inps = parser.parse_args()
    return inps

if __name__ == '__main__':
    '''
    The main driver.
    '''
    inps = parse()

    currDir = os.getcwd()

    if inps.subset in ['', None]:
        pairDirs = SS.getPairDirs(dirname=inps.dirname)
    else:
        pairDirs = SS.pairDirs_from_file(inps.subset, base=inps.dirname)

    print 'Number of IFGs : ', len(pairDirs)
    print 'Start step: ', inps.start
    print 'End step: ', inps.end

    for pair in pairDirs:
        unwfile = os.path.join(pair, 'filt_topophase.unw')
        if os.path.exists(unwfile) and (not inps.force) and (inps.start != 'geocode'):
            print 'Interferogram {0} already processed upto unwrapping'.format(pair)
        else:
            print 'Processing: {0}'.format(pair)

            os.chdir(pair)
            #os.system('pwd')
            syscall = 'insarApp.py insarApp.xml --start={0} --end={1}'.format(inps.start, inps.end)
            os.system(syscall)
            if not inps.noclean:
                os.system('rm -f *.slc* *.raw*')

            os.chdir(currDir)

