#! /usr/bin/env python
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2011, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from __future__ import print_function
import pdb
import os
import sys
from os import path

import logging
import logging.config
logging.config.fileConfig(os.environ['ISCE_HOME']+ '/library/applications/logging.conf')

# get directory that this file is in. this works for py drivers that are not "compiled" into a pyc location.
framePath = path.dirname(__file__)
if framePath not in sys.path:
    sys.path.append(framePath)

from iscesys.Compatibility import Compatibility
Compatibility.checkPythonVersion()
from iscepge.peg_region_check.PegRegionChecker import PegRegionChecker
from iscepge.frame_util.Mover import Mover
import cPickle as cP

#this driver generates the oppurtune pickled file containing, possibly, the peg region

# arguments:
#   argv[0] the operational mode 0 = nominal, 1 = on demand  with predefined peg region, 2 = on demand with dynamic peg
#   argv[1] is the CAS Frame ID for the query for nominal, pickle file with tuple of list of dicts for on demand
#   argv[2] is the directory where to save the file that is saved. it is the staging area for the frame listener
#   argv[3] is the filename of the pickled result (optional, default is "linkPRC_IC_<id>.pck")
#   arg[4] the location of the peg file. it's assume a filename peg_region_xxxx.txt with xxxx = lower(sensor name)
def main(argv):
    #pdb.set_trace()
    minNumArg = 2
    if not (len(argv) >= minNumArg):
        print("Error. Expected at least",minNumArg,"input arguments.")
        raise Exception
    if len(argv) > 4:# contains the peg file location
        pegLocation = argv[4]
    else:
        pegLocation = ''

    print("driverPegRegionChecker: REPLACE THE pegfile_alos.txt_test with one w/ no _test") 
    pegfile = os.path.join(pegLocation,'pegfile_alos.txt_test')
    PRG = PegRegionChecker(pegfile,'ALOSRawFrameData')
    opMode = int(argv[0])
    # if the name is not present use the id, since it is unique, to make one up
    tbp = []
    email = None
    if opMode == 0:
        tbp, pegs = PRG.runNominalMode(argv[1])
    elif opMode == 1:
        tbp, pegs, email = PRG.runOnDemandModeWithPredefinedPeg(argv[1])
    elif opMode == 2:# the pegs now is a list with one element = None
        tbp, pegs , email = PRG.runOnDemandModeWithDynamicPeg(argv[1])



    if tbp == []:
        print("driverCheckPegRegionChecker: not covered")
        return 10
    else:
        if len(argv) > 2:
            cwd = argv[2]
        else:
            cwd = ''
        if len(argv) > 3:
            filename = argv[3]
        else:
            filename = 'linkPRC_IC_' + argv[1] + '.pck'

        # to dedupe the frames pickle file, use the md5 checksum of the contents as the filename so that same frames pickle file are of the same filename.
        import hashlib
        hash = hashlib.md5()
        hashList = []
        for pegRegion in tbp:#list for each matching peg region
            for frameList in pegRegion:# one list is for master the other for slave
                for frame in frameList:# frameList is a list of dictionaries
                    hashList.append(frame['Filename'][0])# Filename is a one element list
        hashList.sort()
        #print('hash list',hashList)
        hashStr = "".join(hashList)
        hash.update(hashStr)
        md5hash = hash.hexdigest()
        #try:
            #print('md5hash',md5hash.str())
        #except:

        filename = md5hash

        filen = os.path.join(cwd,filename)
        fp  = open(filen,'w')
        cP.dump((tbp,pegs,email),fp,2)
        fp.close()
        os.system("touch " + os.path.join(cwd,'ARIAMETA-' + filename + '.signal'))
        print("driverCheckPegRegionChecker: covered")
    return 0


if __name__ == "__main__":
    import sys
    argv = sys.argv[1:]
    sys.exit(main(argv))






