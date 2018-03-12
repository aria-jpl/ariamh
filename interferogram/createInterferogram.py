#! /usr/bin/env python3 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2012, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import sys, traceback


from interferogram.insarMH import InsarMH
from utils.contextUtils import toContext
from interferogram.Interferogram import Interferogram


def createProject(inputs):
    ifg = Interferogram()
    ifg._project = inputs['project']
    
    if(inputs['workflow']  == 'vanilla_isce'):
        ifg._insarClass  = InsarMH

        
    return ifg
def main():
    import json
    inputs = json.load(open(sys.argv[1]))
    '''Command line parsing.'''
    '''
    import argparse
    parser = argparse.ArgumentParser(
            description='Create an unwrapped tropospheric corrected interferogram in two stages.',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
            )

    parser.add_argument('-i','--inputFile', action='store', default=None, dest='inputFile',
            help='The json input file', type=str)
    parser.add_argument('-s','--stage', action='store', default=0, dest='stage',
            help='One of the two stage (0,1)', type=int)
    parser.add_argument('-e','--errorCode', action='store', default=0, dest='errorCode',
            help='The error code of the tropospheric correction step', type=int)
    parser.add_argument('-p','--project', default='', dest='project',
            help='project', type=str)
    ops = parser.parse_args()
    #ifg.createProductJson(ifg.createMetadata(filename))
    '''
    ifg = createProject(inputs)
    ifg.run(inputs)
    


if __name__ == '__main__':
    try: status = main()
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc()) 
        raise
    sys.exit(status)
