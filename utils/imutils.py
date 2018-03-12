import sys
import os
import isce
from isceobj.Image.Image import Image
import numpy as np
from utils.UrlUtils import UrlUtils

__all__ = ['download_data','get_image','get_size','fix_xml','compute_residues',
           'get_water_mask','crop_mask']

def download_data(url):
    uu = UrlUtils()
    command = 'curl -k -f -u' + uu.dav_u + ':' + uu.dav_p + ' -O ' + url
    os.system(command)

def get_image(fname):
    im  = Image()
    im.load(fname)
    return im

def get_size(im):
    if isinstance(im,str):
        im = get_image(im)
    latstart1 = im.coord2.coordStart
    latsize1 = im.coord2.coordSize
    latdelta1 = im.coord2.coordDelta
    lonstart1 = im.coord1.coordStart
    lonsize1 = im.coord1.coordSize
    londelta1 = im.coord1.coordDelta
    return {'lat':{'val':latstart1,'size':latsize1,'delta':latdelta1},
      'lon':{'val':lonstart1,'size':lonsize1,'delta':londelta1}}

def fix_xml(fname):
    fp = open(fname)
    al = fp.readlines()
    fp.close()
    fp = open(fname,'w')
    for l in al:
        fp.write(l.replace('merged/',''))
    fp.close()

def compute_residues(phase):
    a = phase[:,:]/(2*np.pi)
    resid = np.zeros(a[1:,1:].shape,dtype = np.int8)
    mats = [a[:-1,:-1],a[:-1,1:],a[1:,1:],a[1:,:-1]]
    nmats = len(mats)
    for i in range(nmats):
        resid  =  resid + np.round(mats[(i+1)%nmats] - mats[i]).astype(np.int8)
    return resid

def get_water_mask(oname,image):
    latmax = np.ceil(image.coord2.coordStart)
    latmin = np.floor(image.coord2.coordStart + image.coord2.coordSize * image.coord2.coordDelta)
    lonmin = np.floor(image.coord1.coordStart)
    lonmax = np.ceil(image.coord1.coordStart + image.coord1.coordSize * image.coord1.coordDelta)
    bbox = ''.join(str([latmin, latmax, lonmin, lonmax]).split())
    command = 'wbdStitcher.py wbdStitcher.xml wbdstitcher.wbdstitcher.bbox=' + bbox \
                + ' wbdstitcher.wbdstitcher.outputfile=' + oname
    print('running',command)
    if os.system(command) != 0:
        print("Error")

def crop_mask(im1,im2,outname):
    latstart1 = im1.coord2.coordStart
    latsize1 = im1.coord2.coordSize
    latdelta1 = im1.coord2.coordDelta
    lonstart1 = im1.coord1.coordStart
    lonsize1 = im1.coord1.coordSize
    londelta1 = im1.coord1.coordDelta
    latstart2 = im2.coord2.coordStart
    latsize2 = im2.coord2.coordSize
    latdelta2 = im2.coord2.coordDelta
    lonstart2 = im2.coord1.coordStart
    lonsize2 = im2.coord1.coordSize
    londelta2 = im2.coord1.coordDelta
    ilatstart = abs(int(round((latstart2-latstart1)/latdelta2)))
    ilatend = ilatstart + latsize1
    ilonstart = abs(int(round((lonstart2-lonstart1)/londelta2)))
    ilonend = ilonstart + lonsize1
    imIn = im2.memMap(band=0)
    imCrop = np.memmap(outname,im2.toNumpyDataType(),'w+',shape=(latsize1,lonsize1))    
    imCrop[:,:] =  imIn[ilatstart:ilatend,ilonstart:ilonend]
    im3 = im2.clone()
    im3.filename = outname
    im3.coord2.coordStart = latstart1 
    im3.coord2.coordSize = latsize1 
    im3.coord2.coordDelta = latdelta1 
    im3.coord2.coordEnd = latstart1 + latsize1*latdelta1
    im3.coord1.coordStart = lonstart1 
    im3.coord1.coordSize = lonsize1 
    im3.coord1.coordDelta = londelta1 
    im3.coord1.coordEnd = lonstart1 + lonsize1*londelta1
    im3.renderHdr()
    return np.copy(imCrop)


