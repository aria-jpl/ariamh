#!/usr/bin/env python
from __future__ import absolute_import, print_function, division
import pylab as pl
import sys
from imtiler import *
from imtiler.util import *
from ariaml_util import *

unwpng = 'filt_topophase.unw.geo.browse.png'
cohpng = 'topophase_ph_only.cor.geo.browse.png'

from skimage.transform import resize as imresize
def rotate_bbox(bbox_xy,snap=1.0):
    '''
    computes rotation matrix minimizing *width* of bounding box
    '''
    from numpy import cos, sin, arange, abs, dot, radians
    rot_xy = bbox_xy
    rot_mat = [[1.0,0.0],[0.0,1.0]]
    rot_deg = 0.0
    if snap==0:
        return rot_xy, rot_mat, rot_deg

    xr = extrema(bbox_xy[0,:])
    rot_min = abs(xr[1]-xr[0])
    for r in arange(-90,91,snap):
        ar    = radians(r)
        cosr  = cos(ar)
        sinr  = sin(ar)
        r_ar   = [[cosr,-sinr], [sinr,cosr]]
        r_xy   = dot(r_ar,bbox_xy)
        xr     = extrema(r_xy[0,:])
        r_diff = abs(xr[1]-xr[0])
        # NOTE (BDB, 09/01/15): code below finds min size (not min width) bbox
        # yr = extrema(rxy[1,:])
        # r_diff = np_min([xr[1]-xr[0],yr[1]-yr[0]]) 
        if r_diff < rot_min:
            rot_min = r_diff
            rot_xy,rot_mat,rot_deg  = r_xy,r_ar,r

    return rot_xy, rot_mat, rot_deg

def rotate_crop_geo(ifg,coh):
    # rotate and crop zero boundaries of orthorectified IFGs
    # to extract more informative tiles 
    from skimage.transform import rotate as imrotate
    from skimage.segmentation import find_boundaries
    assert(ifg.dtype==np.float32 and coh.dtype == np.float32)
    zero_bounds = find_boundaries((ifg!=0).all(axis=2),mode='thick')
    bbox_xy = np.c_[np.where(zero_bounds.T)].T
    rot_xy, rot_mat, rot_deg = rotate_bbox(bbox_xy,snap=1.0)    
    ifg = imrotate(ifg,-rot_deg,preserve_range=True)
    coh   = imrotate(coh,-rot_deg,preserve_range=True)
    nonzero =  (ifg!=0).any(axis=2)
    keeprows = nonzero.any(axis=1)
    keepcols = nonzero.any(axis=0)
    ifgout = np.uint8(ifg[:,keepcols,:][keeprows]*255)
    cohout = coh[:,keepcols][keeprows]
    print(extrema(coh),extrema(cohout))
    return ifgout,cohout

if __name__ == '__main__':
    import argparse
    import numpy as np
    
    parser = argparse.ArgumentParser(description='IFG tiler')
    parser.add_argument('-d','--dim', type=int, default=256,
                       help='Tile dimension')
    parser.add_argument('-n','--numtiles', type=int, default=25,
                       help='Max number of tiles to extract from each image')
    parser.add_argument('-a','--accept', type=str, default='mask',
                        help='% of valid pixels to accept  (default=\'mask\')')
    parser.add_argument('-r','--replacement', action='store_true',
                       help='Sample image tiles with replacement')
    parser.add_argument('-o','--outdir', default='./ifg_tile_cache/',
                        type=str, help='Output directory for image tiles')    
    parser.add_argument('-p','--plot', action='store_true',
                       help='Plot summary images')
    parser.add_argument('-c','--clobber', action='store_true',
                       help='Overwrite image tiles if they already exist')
    parser.add_argument('-e','--ext', type=str, default='.png',
                       help='Output file extension')    
    parser.add_argument('-t','--transpose', action='store_true',
                       help='Transpose interferogram rows/cols before tiling')
    parser.add_argument('-v','--verbose', action='store_true',
                       help='Enable verbose output')       
    parser.add_argument('-m','--mask', type=float, default=0.4,
                       help='Coherence mask threshold')   
    parser.add_argument('unw', type=str, metavar='UNW',
                        help='Unwrapped Interferogram (browse image) to tile')
    parser.add_argument('coh', type=str, metavar='COH',
                        help='Coherence image corresponding to unw')
    args = parser.parse_args()

    # reproducibility!
    np.random.seed(42)

    verbose = args.verbose
    
    # tile parameters
    numtiles = args.numtiles
    tiledim  = args.dim
    tpose    = args.transpose
    accept   = args.accept
    replace  = args.replacement
    clobber  = args.clobber
    doplot   = args.plot
    
    unwf = args.unw
    cohf = args.coh
    assert(unwf.endswith(unwpng))
    assert(cohf.endswith(cohpng))
    
    ifgbase,ifgext = splitext(unwf)
    
    # output dir / extension for tile ifgs
    tilerootdir = args.outdir
    tileext = args.ext 

    maskthr  = args.mask
    
    ifg = loadfunc(unwf)[:,:,:3]
    cohrgba = np.float32(loadfunc(cohf))
    if cohrgba.shape[:2] != ifg.shape[:2]:
        cohrgba = imresize(cohrgba,ifg.shape[:2],preserve_range=True,order=0)

    ifg,cohrgba = rotate_crop_geo(ifg,np.float32(cohrgba))
    coh =  cohrgba2intensity(cohrgba.copy()).squeeze()
    cohrgba = np.uint8(cohrgba*255)
    
    if tpose:
        print('transposing ifg',ifg.shape)
        ifg = ifg.transpose((1,0,2))
        coh = coh.transpose((1,0))
        cohrgba = cohrgba.transpose((1,0,2))

    cohmask = coh>=maskthr

    if doplot:
        fig,ax = pl.subplots(1,4,sharex=True,sharey=True)
        ax[0].imshow(ifg)
        ax[1].imshow(np.uint8(cohmask))
        ax[2].imshow(coh)
        ax[3].imshow(cohrgba)
    tiler = MaskTiler(cohmask,tiledim,numtiles=numtiles,accept=accept,
                      replacement=replace,verbose=verbose)
    ul = tiler.collect()
        
    ifgbase = pathsplit(unwf)[0].split('/')[-1]
    unwbase = splitext(basename(unwf))[0]
    cohbase = splitext(basename(cohf))[0]

    for img,imgbase in ((ifg,unwbase),(cohrgba,cohbase)):
        tiledir = pathjoin(tilerootdir,str(tiledim),ifgbase,imgbase)
        if not pathexists(tiledir):
            os.makedirs(tiledir)
        tilef = save_tiles(img,ul,tiledim,tiledir,tileext,savefunc,
                           outprefix='tile',overwrite=clobber)
        if doplot:
            plot_tiles(img,ul,tiledim,mask=cohmask)

    if doplot:
        pl.show()
    
    sys.exit(0)
    
