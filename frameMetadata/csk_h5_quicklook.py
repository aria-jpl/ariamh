#!/usr/bin/env python

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#                        NASA Jet Propulsion Laboratory
#                      California Institute of Technology
#                        (C) 2013  All Rights Reserved
#
# Copyright [2011]. California Institute of Technology.  ALL RIGHTS RESERVED.
# U.S. Government sponsorship acknowledged. Any commercial use must be
# negotiated with the Office of Technology Transfer at the California
# Institute of Technology.
#
# This software is subject to U. S. export control laws and regulations
# (22 C.F.R. 120-130 and 15 C.F.R. 730-774). To the extent that the
# software is subject to U.S. export control laws and regulations, the
# recipient has the responsibility to obtain export licenses or other
# export authority as may be required before exporting such information
# to foreign countries or providing access to foreign nationals.
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


import sys
from os import path
from datetime import datetime
from optparse import OptionParser

import h5py
import numpy

# -------------------------------------------------------------------------
# configure Singleton loggers
# -------------------------------------------------------------------------

import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG) # use WARNING as default for non-verbose mode
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler) # do not want to potentially enable full verbose to stdout.



def showUsage(thisCommand):
    print '\n Extracts a quick look dataset from CSK h5 and exports to a raster image file.\n'
    print '  Usage: %s [options]  {csk_h5}  {browse_image} \n' % (thisCommand)
    print '  Examples:\n'
    print '      %s  CSKS1_RAW_B_HI_01_HH_RD_SF_20130629021016_20130629021023.h5  CSKS1_RAW_B_HI_01_HH_RD_SF_20130629021016_20130629021023.png' % (thisCommand)
    print '''  Options:
    -h/--help               This help text.
    -v/--verbose            Verbose output.
    -d/--debug              Verbose with debug output. Assumes verbose.
    -s/--settings           The settings json file.
'''
    print '''  Arguments
    csk_h5    the CSK h5 file to read.
    browse_image  a browse image to export to.
'''
# end def


def h5_to_browse(h5Filepath, browseFilepath):
    '''
    Exports a HDF5 quick look dataset 'S01/QLK' to a browse raster image file. 
    '''
    with h5py.File(h5Filepath,'r') as f:

        # show contents
        def visitCallback(item):
            logger.info('%s' % (str(item)))
        # end def
        f.visit(visitCallback)

        # get quicklooks dataset
        datasetQuicklook = None
        try:
            datasetQuicklook = f['S01/QLK']
            logger.info('found quicklook dataset (S01/QLK) : %s' % str(datasetQuicklook))
        except Exception, e:
            logger.error('unable to read quicklook dataset (S01/QLK) in %s: %s' % (h5Filepath, str(e)) )
            raise Exception('unable to read quicklook dataset (S01/QLK) in %s.' % h5Filepath)
        # end try-except

        # save array to png
        try:
            import scipy.misc
            scipy.misc.imsave(browseFilepath, datasetQuicklook)
            logger.info('exported quicklook dataset (S01/QLK) to %s' % str(browseFilepath))
        except Exception, e:
            logger.error('unable to export quicklook dataset (S01/QLK) to %s: %s' % (str(browseFilepath), str(e)) )
            raise Exception('unable to export quicklook dataset (S01/QLK) to %s' % str(browseFilepath))
        # end try-except

#        from matplotlib import pyplot as plt
#        plt.imshow(datasetQuicklook, interpolation='nearest')
#        plt.show()
    # end with f
    
# end def

if __name__ == '__main__':

    # -------------------------------------------------------------------------
    # get command line input options and arguments
    # -------------------------------------------------------------------------

    from getopt import getopt
    from getopt import GetoptError

    try:
        # optlist is list of (option, value)
        # args is list of arguments, not including options.
        # http://docs.python.org/library/getopt.html
        (optlist, args) = getopt(sys.argv[1:], 'hvds:c:', ['help','verbose','debug','settings='])
    except GetoptError, e:
        print >>sys.stderr, str(e)
        print >>sys.stderr, "for help use --help"
        sys.exit(2)
    # end try-except

    # the name of this command
    thisCommandFilepath = path.abspath( sys.argv[0] )
    basePath = path.dirname( path.dirname(thisCommandFilepath) )
    logPath = path.join(basePath, 'logs')
    confPath = path.join(basePath, 'conf')

    # default option values
    logFilePath = None
    settingsFilepath = path.join(confPath,'settings.json')

    # handle command-line options
    for (option, value) in optlist:
        if option in ('-h', '--help'):
            showUsage(sys.argv[0])
            sys.exit(2)
        elif option in ('-v', '--verbose'):
            # lower logging level
            logger.setLevel(logging.INFO)
        elif option in ('-d', '--debug'):
            # lower logging level
            logger.setLevel(logging.DEBUG)
        elif option in ('-s', '--settings'):
            settingsFilepath = value
        # end if
    # end for
    

    # if have less than the number of required arguments, then show usage.
    # args is a list of arguments (not including the executable and options).
    if (len(args) < 2):
        print >>sys.stderr, 'Insufficient arguments %s' % str(args)
        print >>sys.stderr, "For help use -h or --help"
        sys.exit(1)
    # end if

    # -------------------------------------------------------------------------
    # update logger based on command-line input
    # -------------------------------------------------------------------------

    if logFilePath:
        # add logger handler for file output
        handler = logging.FileHandler(logFilePath)
        handler.setLevel(logging.NOTSET) 
        formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    # end if

    # -------------------------------------------------------------------------
    # initialize command line input arguments
    # -------------------------------------------------------------------------

    h5Filepath = None
    try:
         h5Filepath = args[0]
    except IndexError, e:
        logger.error('unable to get argument for "h5Filepath": %s' % str(e) )
    # end try-except

    browseFilepath = None
    try:
         browseFilepath = args[1]
    except IndexError, e:
        logger.error('unable to get argument for "browseFilepath": %s' % str(e) )
    # end try-except

    # -------------------------------------------------------------------------
    # validate input arguments
    # -------------------------------------------------------------------------

    # convert to absolute paths
    if path.isfile(h5Filepath):
        h5Filepath = path.abspath(h5Filepath)
        logger.info('h5Filepath: %s' % (h5Filepath))
    else:
        logger.error('h5Filepath "%s" is not a valid file path.' % h5Filepath)
        sys.exit(2)
    # end if


    try: # with check for KeyboardInterrupt

        # -------------------------------------------------------------------------
        # main
        # -------------------------------------------------------------------------

        # exports a CSK HDF5 quick look dataset to a browse raster image file.
        try:
            h5_to_browse(h5Filepath, browseFilepath)
        except Exception, e:
            logger.error('unable to create browse image: %s' % str(e))
        # end try-except

        sys.exit(0)

# -----------------------
#        # import psyco if available
#        try:
#            import psyco
#            psyco.full()
#            print '**** using psyco ****'
#        except ImportError, e:
#            print '**** not using psyco ****'
#        # end try-except

# -----------------------
#        # profiling main()
#        statsFilename = '~profile.stats'
#
#        import profile
#        profile.run('main()', statsFilename )
#
#        import pstats
#        stats = pstats.Stats(statsFilename)
#        stats.sort_stats('time').print_stats()
#
#        sys.exit(0)

# -----------------------

    except SystemExit, e:
        # sys.exit() throws exception SystemExit with exit value
        logging.shutdown()
        print '\n'
        #print '# %s Exiting main() with return value: %s' % ( str(datetime.now()), str(e) )

    except KeyboardInterrupt, e:
        print >>sys.stderr, '\n'
        print >>sys.stderr, '# ---------------------------------------------------'
        print >>sys.stderr, '# PROCESS CANCELLED BY USER. Traceback:'
        # get the stack trace
        import traceback
        traceback.print_exc(file=sys.stderr)
        print >>sys.stderr, '# ---------------------------------------------------'
        sys.exit(2)

    except Exception, e:
        print >>sys.stderr, '\n'
        print >>sys.stderr, '# ---------------------------------------------------'
        print >>sys.stderr, '# %s Exception uncaught at main():' % ( str(e) )
        print >>sys.stderr, '# %s' % (str(e))
        print >>sys.stderr, '# Traceback:'
        # get the stack trace
        import traceback
        traceback.print_exc(file=sys.stderr)
        print >>sys.stderr, '# ---------------------------------------------------'
        sys.exit(2)

    # end try-except

# end if
