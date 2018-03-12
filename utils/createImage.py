#! /usr/bin/env python3: 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Author: Giangi Sacco
# Copyright 2015, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import os
import math
import matplotlib.image as mpimg
from subprocess import check_call
import traceback
import logging
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('createImage')
def call_noerr(cmd):
    """Run command and warn if exit status is not 0."""

    try: check_call(cmd, shell=True)
    except Exception as e:
        logger.warn("Got exception running {}: {}".format(cmd, str(e)))
        logger.warn("Traceback: {}".format(traceback.format_exc()))
def createImage(command,item):
    #print(command)
    max_width = 800
    max_width_small = 300
    name1 = item + '.png'
    final = item + '.browse.png'
    finalSmall = item + '.browse_small.png'
    call_noerr(command)
    call_noerr('convert out.ppm -transparent black ' + name1)
    im = mpimg.imread(name1)
    #if there is alpha channel set transparency to one (makes it black) where all data are zero
    if im.shape[2] == 4:
        im[im[:,:,3] == 0,3] = 1
    length = im.shape[0]
    width = im.shape[1]
    #reduce the width to max 512 for normal size and 128 for small size
    resamp = 1 if width < max_width else int(width/max_width)
    if(resamp == 1):
        call_noerr('cp ' + name1 + ' ' + final)
    else:
        mpimg.imsave(final,im[::resamp,::resamp,:])
   
    resamp = 1 if width < max_width_small else int(width/max_width_small)
    if(resamp == 1):
        call_noerr('cp ' + name1 + ' ' + finalSmall)
    else:
        mpimg.imsave(finalSmall,im[::resamp,::resamp,:])
    if os.path.exists('out.ppm'):
        os.unlink('out.ppm')
    if os.path.exists(name1):
        os.unlink(name1)
