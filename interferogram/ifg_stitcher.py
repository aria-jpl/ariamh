#!/usr/bin/env python3 
from scipy.ndimage.morphology import binary_dilation
from scipy.ndimage import generate_binary_structure
import numpy as np
from matplotlib import pyplot as plt
plt.ion()
import sys
import os
import isce
from isceobj.Image.Image import Image
from isceobj.Image.BILImage import BILImage
from utils.imutils import *
from utils.UrlUtils import UrlUtils
import shutil
import argparse
import json
import tempfile
import copy
from contrib.UnwrapComp.unwrapComponents import UnwrapComponents

WATER_VALUE = 255
#if changing the WATER_VALUE back to negative chenge this line np.logical_and(uim1 > 0,uim1 < WATER_VALUE)

class IfgStitcher:
    def __init__(self):
        #isce image object of the full mask
        self._wmask = None
        self._small = 1e-20
        self._minthr = .85
        self._debug = False
        self._keepth = 10000
        self._cor_name = 'phsig.cor.geo'
        self._extra_prd_names = []
        self._extra_prds_out = []
        self._extra_prds_in1 = []
        self._extra_prds_in2 = []
        self._image_info = {}
        self._stitch_only = False


#zero the multiples of np in the overlap region
    def zero_n2pi_full(self,im):
        minv = np.min(im)
        maxv = np.max(im)
        eps = 0.01 #might need to play a bit with this value  
        #subtract small amount to make sure don't miss case in which it's very close
        i = int((minv - .1)/(2*np.pi))
        while(2*np.pi*i < maxv):
            toz = np.nonzero(np.abs(im - 2*i*np.pi) < eps)
            im[toz] = 0
            i += 1
        return im

    def overlap(self,im,overlap_mask,wmsk1,i0,j0,use_res=False):
        if use_res:
            res = compute_residues(im)
            res = binary_dilation(res.astype(np.int8),structure=generate_binary_structure(2,2),iterations=10)
            xres = np.zeros(im.shape,np.int32)
            xres[1:,1:] = res
            #select only  point that are non zero and zero residue
            msk = np.nonzero(np.logical_and(np.logical_and(np.abs(im) > self._small,np.abs(xres) == 0),wmsk1 == 0))
        else:
            msk = np.nonzero(np.logical_and(np.abs(im) > self._small,wmsk1 == 0))
    
        overlap_mask[i0 + msk[0],j0 + msk[1]] += 1
        return overlap_mask
    
    def get_ovelap(self,ims,wmsks,length,width,i0,j0,use_res=False):
        overlap_mask = np.zeros([length,width],np.int8)
        overlap_mask = self.overlap(ims[0],overlap_mask,wmsks[0],i0[0],j0[0],use_res)
        overlap_mask = self.overlap(ims[1],overlap_mask,wmsks[1],i0[1],j0[1],use_res)
        #whatever in overlap_mask has value of 2 is in the overlap region
        over = np.nonzero(overlap_mask == 2)
        return over,overlap_mask
    
    def save_image(self,input_template,outname,size):
        im  = Image() 
        im.load(input_template + '.xml')
        latstart = size['lat']['val']
        lonstart = size['lon']['val']
        latsize = size['lat']['size']
        lonsize = size['lon']['size']
        latdelta = size['lat']['delta']
        londelta = size['lon']['delta']
        im.filename = outname
        im.coord2.coordStart = latstart 
        im.coord2.coordSize = latsize
        im.coord2.coordDelta = latdelta 
        im.coord2.coordEnd = latstart + latsize*latdelta
        im.coord1.coordStart = lonstart
        im.coord1.coordSize = lonsize
        im.coord1.coordDelta = londelta 
        im.coord1.coordEnd = lonstart + lonsize*londelta
        im.renderHdr()
    
    def create_wbd_template(self):
        fp = open('wbdStitcher.xml','w')
        fp.write('<stitcher>\n')
        fp.write('    <component name="wbdstitcher">\n')
        fp.write('        <component name="wbd stitcher">\n')
        fp.write('            <property name="url">\n')
        fp.write('                <value>https://urlToRepository</value>\n')
        fp.write('            </property>\n')
        fp.write('            <property name="action">\n')
        fp.write('                <value>stitch</value>\n')
        fp.write('            </property>\n')
        fp.write('            <property name="directory">\n')
        fp.write('                <value>outputdir</value>\n')   
        fp.write('            </property>\n')
        fp.write('            <property name="bbox">\n')
        fp.write('                <value>[33,36,-119,-117]</value>\n')   
        fp.write('            </property>\n')
        fp.write('            <property name="keepWbds">\n')
        fp.write('                <value>False</value>\n')  
        fp.write('            </property>\n')
        fp.write('            <property name="noFilling">\n')
        fp.write('                <value>False</value>\n')   
        fp.write('            </property>\n')
        fp.write('        </component>\n')
        fp.write('    </component>\n')
        fp.write('</stitcher>')
        fp.close()
    #create the masks that cover all the region and then return slices of it
    def create_mask(self,sizes):
        minlat = 1000
        minlon = 1000
        maxlat = -1000
        maxlon = -1000
        for i in sizes:
            for j in i:
                #delta for lat is negative while for lon positive that's why 
                #one applies to min and the other to max
                if j['lat']['val'] + j['lat']['size']*j['lat']['delta'] < minlat:
                    minlat = j['lat']['val'] + j['lat']['size']*j['lat']['delta']
                if j['lat']['val'] > maxlat:
                    maxlat = j['lat']['val']
                if j['lon']['val'] < minlon:
                    minlon = j['lon']['val']
                if j['lon']['val'] + j['lon']['size']*j['lon']['delta'] > maxlon:
                    maxlon = j['lon']['val'] + j['lon']['size']*j['lon']['delta']
        
        if not self._debug:
            tf = tempfile.NamedTemporaryFile()
            tf.close()
            oname = tf.name
        else:
            oname = 'wbdmask.wbd'
        if not os.path.exists(oname):
            self.create_wbd_template()
            bbox = ''.join(str([ int(np.floor(minlat)),  int(np.ceil(maxlat)),  int(np.floor(minlon)), int(np.ceil(maxlon))]).split())
            uu = UrlUtils()
            command = 'wbdStitcher.py wbdStitcher.xml wbdstitcher.wbdstitcher.bbox=' + bbox \
                    + ' wbdstitcher.wbdstitcher.outputfile=' + oname \
                    + ' wbdstitcher.wbdstitcher.url=' + uu.wbd_url
            if os.system(command) != 0:
                print("Error creating water mask")
                raise Exception
        self._wmask = get_image(oname + '.xml')
         
    #get a list of input files and return 2d array with the ordered in lat and lon going
    #top left bottom right. assume list of lists and each element arranged by subswaths
    #[[ifg1_sw1,ifg1_sw2,ifg1_sw3],...,[ifgn_sw1,ifgn_sw2,ifgn_sw3]]
    def arrange_frames(self,fnames):
        sizes = []
        alats = []
        blats = []
        alons = []
        for names in fnames:
            sz = []
            lats = []
            lons = []
            blat = [] 
            for name in names:
                size = get_size(name + '.xml')
                sz.append(size)
                lats.append(size['lat']['val'])
                lons.append(size['lon']['val'])
                blat.append(lats[-1] + size['lat']['delta']*size['lat']['size'])
            sizes.append(sz)
            alats.append(lats)
            alons.append(lons)
            blats.append(blat)
        alats = np.array(alats)
        alons = np.array(alons)
        blats = np.array(blats)
        if not self.check_overlap(blats,alats):
            return None,None
        #just need one col (lat) and one row (lon) to determine the order
        lat = alats[:,0]
        lon = alons[0,:]
        ilat = np.argsort(lat)[::-1]
        ilon = np.argsort(lon)
        rsizes = []
        rnames = []
        for i in ilat:
            sz = []
            names = []
            for j in ilon:
                sz.append(sizes[i][j])
                names.append(fnames[i][j])
            rsizes.append(sz)
            rnames.append(names)
        return rnames,rsizes
    
    def crop_mask(self,size1,im2,outname):
        latstart1 = size1['lat']['val']
        latsize1 = size1['lat']['size']
        latdelta1 = size1['lat']['delta']
        lonstart1 = size1['lon']['val']
        lonsize1 = size1['lon']['size']
        londelta1 = size1['lon']['delta']
        latstart2 = im2.coord2.coordStart
        latsize2 = im2.coord2.coordSize
        latdelta2 = im2.coord2.coordDelta
        lonstart2 = im2.coord1.coordStart
        lonsize2 = im2.coord1.coordSize
        londelta2 = im2.coord1.coordDelta
        #if mask has diffrent resolution then image
        factor = int(round(londelta1/londelta2))

        ilatstart = abs(int(round((latstart2-latstart1)/latdelta2)))
        ilatend = ilatstart + factor*latsize1
        ilonstart = abs(int(round((lonstart2-lonstart1)/londelta2)))
        ilonend = ilonstart + factor*lonsize1
        imIn = im2.memMap(band=0)
        imCrop = np.memmap(outname,im2.toNumpyDataType(),'w+',shape=(latsize1,lonsize1))    
        imCrop[:,:] =  imIn[ilatstart:ilatend:factor,ilonstart:ilonend:factor]
        return np.copy(imCrop)
    
    #create a memmap. if filename is empty create a tempfile
    def get_memmap(self,dtype,mode,shape,filename=''):
        if not filename:
            fp = tempfile.NamedTemporaryFile()
            filename = fp.name
            fp.close()
            
        return np.memmap(filename, dtype=dtype, mode=mode, shape=shape)
  
    #find out which image should be used as a reference to adjust the conncomp
    #main idea is to see which one covers a large portions with the list number of
    #conncomps.
    #return which image convers the most [0 or 1] and the connected components
    #in the two image ovelaps, without the zero  

    def ref_image(self,imo1,imo2,factor=1):
        #find the connected components in the overlap
        uim1 = np.unique(imo1)
        #remove the -1
        uim1 = uim1[np.logical_and(uim1 > 0,uim1 < WATER_VALUE)]
        uim2 = np.unique(imo2)
        uim2 = uim2[np.logical_and(uim2 > 0, uim2 < WATER_VALUE)]
        cover1 = []
        for i in uim1:
            cover1.append(np.nonzero(imo1 == i)[0].size)
        cover1 = np.array(cover1)
        
        sel = cover1 > self._keepth/factor
        discard1 = uim1[np.logical_and(np.logical_not(sel),cover1 > self._keepth/(2*factor))]
        uim1 = uim1[sel]
        cover1 = cover1[sel]
        if len(cover1) == 0:
            return -1,None,None,None,None
        cover2 = []
        for i in uim2:
            cover2.append(np.nonzero(imo2 == i)[0].size)
        cover2 = np.array(cover2)
        sel = cover2 > self._keepth/factor
        discard2 = uim2[np.logical_and(np.logical_not(sel),cover2 > self._keepth/(2*factor))]
        uim2 = uim2[sel]
        cover2 = cover2[sel]
        if len(cover2) == 0:
            return -1,None,None,None,None
        sc1 = np.sum(cover1)
        sc2 = np.sum(cover2)
        ret = -1
        #find which one covers the most with the least number of conncomp
        if sc1  > self._minthr and sc2 > self._minthr:
            #they both cover the minimun required, use the min of the two as threshold
            thr = min(sc1,sc2)
            cs1 = np.cumsum(cover1)
            cs2 = np.cumsum(cover2)
            indx1 = np.nonzero(cs1 >= thr - self._small)[0]
            indx2 = np.nonzero(cs2 >= thr - self._small)[0]
            #should always be true but sanity check
            if len(indx1) > 0 and len(indx2) > 0:
                if indx1[0] < indx2[0]:
                    ret = 0
                elif indx1[0] > indx2[0]:
                    ret = 1               
                else:#they are =, so find the one that has the most at that indx
                    if(cs1[indx1[0]] > cs2[indx2[0]]):
                        ret = 0
                    else:
                        ret = 1

        elif sc1 > self._minthr:
            #check if first has at least required coverage
            ret = 0
        elif sc2 > self._minthr:
            #check if second has at least required coverage
            ret = 1
        #else will leave it to -1       
        return ret,uim1,uim2,discard1,discard2
    
    def fix_amps(self,imamp,im1amp):  
        #amplitudes have huge outliers. remove them
        seln0 = np.nonzero(imamp != 0)
        mn = np.mean(imamp[seln0])
        st = np.std(imamp[seln0])
        sel1 = np.nonzero(imamp[seln0] > mn + 3*st)
        imamp[seln0[0][sel1],seln0[1][sel1]] = mn + 3*st
        sel1 = np.nonzero(imamp[seln0] < mn - 3*st)
        imamp[seln0[0][sel1],seln0[1][sel1]] = mn - 3*st
        seln0 = np.nonzero(im1amp != 0)
        mn = np.mean(im1amp[seln0])
        st = np.std(im1amp[seln0])
        sel1 = np.nonzero(im1amp[seln0] > mn + 3*st)
        im1amp[seln0[0][sel1],seln0[1][sel1]] = mn + 3*st
        sel1 = np.nonzero(im1amp[seln0] < mn - 3*st)
        im1amp[seln0[0][sel1],seln0[1][sel1]] = mn - 3*st
        return imamp,im1amp
    
    
    def adjust_rest_conncomp(self,im,cim,ccomp_done,offset,addcc):
        """
        Apply offset to all the conncomp that are not part of done
        im = input image
        cim = conncomp image
        ccomp_done = connected components already adjusted
        offset = partial offset to be applied. still need to calculate the mean of teh input image
                 for the particular ccomp
        addcc = offset to add to the conncomp to make them unique
        return the adjusted image
        """
        ucomp = np.unique(cim[::10,::10])
        #leave the -1 untouched and change the zero sepatately since we don't want to
        #change the ccomp number 
        for cc in ucomp:
            if cc in ccomp_done or cc == 0 or cc == WATER_VALUE:
                continue
            sel = cim == cc
            toffset = offset - np.mean(im[sel])
            cim[sel] += addcc
            im[sel] += self.get_offset(toffset)
        sel = cim == 0
        #set the zero conncomp to zero
        im[sel] = 0
        return im
    
    def remove_small_cc(self,cim,im):
        'Absorb small conncomp with the largest'
        ucc = np.unique(cim[::10,::10])
        largest = 0
        sels = []
        lsel = None
        luc = -1 
        for cc in ucc:
            sel = np.nonzero(cim == cc)
            num = sel[0].size
            if num < self._keepth/2:
                sels.append(sel)
            if num > largest:
                largest = num
                lsel = sel
                luc = cc
        mean = np.mean(im[lsel])
        for sel in sels:
            cim[sel] = luc
            im[sel] += mean - np.mean(im[sel])
        
        return
      
    def get_offset(self,offset):
        return offset #2*np.pi*np.round(offset/(2*np.pi))
      
    def adjust_conncomp(self,which,ims,cims,imos,cimos,uccs,discs):
        """
        Adjust the connected component in the ovelap region.
        which = which image is reference and which will be changed
        ims = list of two images
        cims = list of two conncomp images
        imos = point in the overlap region in the two iamges
        cimos = points in the overlap region in the two conncomp images.
        returns the names of the new connected components 
        For each conncomp of the image to be changed it finds the conncomp in the
        reference image that has the largest overlap and offsets the former by the latter
        on that conncomp
        """
        if which == 0:
            k1 = 0
            k2 = 1
        else:
            k1 = 1
            k2 = 0
        '''
        #adjust the zero component which is the low correlation one
        cond1 = cimos[k1] == 0
        cond2 = cimos[k2] == 0
        sel = cims[k2] == 0
        offset = np.mean((imos[k1] - imos[k2])[np.logical_and(cond1,cond2)])
        ims[k2][sel] += offset
        '''
        #save the offset of the largest conncomp of the image and
        #apply it to all the conncomp that were not
        #in the uccs[k2] list (which contains all the conncomp in the overlap region
        #except for the zero one
        ccsize = 0
        ccoffset = 0
        #keep a mapping of the ols and new component values
        newcomps = [{},{}]
        newcomps[k1] = {u1:u1 for u1 in uccs[k1]}
        selc = {}
        ucom2 = np.unique(cims[k2][::10,::10])
        for u2 in uccs[k2]:
            #for each of conncomp in the worst image see how much is covered by each
            #conncomp of the best. the one that covers the most is used to re offset
            #that part of the image 
            cond2 = cimos[k2] == u2
            maxv = 0
            bestc = None
            newcomp = -1
            for u1 in uccs[k1]:
                cond1 = cimos[k1] == u1
                num = np.nonzero(np.logical_and(cond2,cond1))[0].size
                if num > maxv:
                    maxv = num
                    bestc = cond1
                    newcomp = u1
            if bestc is None:
                continue
            #compute the offset in the overlap region.
            offset = np.mean((imos[k1] - imos[k2])[np.logical_and(bestc,cond2)])
            sel = cims[k2] == u2
            #save the offset that has teh largest overlap
            tmp_size = np.nonzero(sel)[0].size
            if tmp_size > ccsize:
                ccoffset = np.mean(imos[k1][bestc])
                ccsize = tmp_size
            ims[k2][sel] += self.get_offset(offset)
            #cims[k2][sel] = newcomp
            #cannot update the newcomp yet because it might become the same as an existing one
            #first update with adjust_rest_conncomp then update
            selc[u2] = sel      
            newcomps[k2][u2] = newcomp
        tmp_unique = np.unique(cims[k1][::10,::10])
        sel = tmp_unique != WATER_VALUE
        addcc = np.max(tmp_unique[sel])
        ims[k2] = self.adjust_rest_conncomp(ims[k2],cims[k2],uccs[k2],ccoffset,addcc)
        ims[k1] = self.adjust_rest_conncomp(ims[k1],cims[k1],uccs[k1],ccoffset,0)
        for k,v in selc.items():
            #make sure that there is not already a component with the same value
            #make sure that u2 is also not one that needs to change. if so do not
            #modify it    
            u2 = newcomps[k2][k]
            if ((u2 in ucom2) and (u2 not in newcomps[k2].values()) or 
                u2 in discs[k2]):
                sel  = np.nonzero(np.diff(ucom2) > 1)[0]
                #reuse some of the gaps in numbering. if no gap use the last one and add 1
                if len(sel):
                    nu2 = ucom2[sel[0]] + 1
                else:
                    nu2 = ucom2[-1] + 1
                if (u2 in ucom2) and (u2 not in newcomps[k2].values()):
                    sel = cims[k2] == u2
                    cims[k2][sel] = nu2
                if u2 in discs[k2]:
                    #update also the discs since it's used after
                    discs[k2][discs[k2] == u2] = nu2
            cims[k2][v] = u2
          
        #go back to each conncomp that was too small and see we can adjust them
        for i in [k1,k2]:
            j = 1 - i#the other index
            for u1 in discs[i]:
                #find the point that have ccomp = u1
                cond1 = cimos[i] == u1
                ncomu = np.unique(np.array(list(newcomps[j].values())))
                #since we have renamed some of the components we might have more than
                #one contributing. keep track with maps
                ncomd = {i:0 for i in ncomu}
                conds = {i:[] for i in ncomu}
                #check which conncomp has the largest overlap. more old ones can have 
                #now the same value
                for uc in uccs[j]:
                    if uc not in newcomps[j]:
                        continue
                    cond2 = cimos[j] == uc
                    nel = np.nonzero(np.logical_and(cond1,cond2))[0].size
                    ncomd[newcomps[j][uc]] += nel
                    conds[newcomps[j][uc]].append(cond2)
                #find the best
                maxv = 0
                bestc = None
                maxc = 0
                for k,v in ncomd.items():
                    if v > maxv:
                        maxv = v
                        bestc = conds[k]
                        maxc = k
                if bestc is not None:
                    #change image and conncomp value
                    for bst in bestc:
                        sel = cims[i] == u1
                        selo = np.nonzero(np.logical_and(bst,cond1))[0]
                        if len(selo):   
                            ims[i][sel] += np.mean((imos[j] - imos[i])[selo])
                            cims[i][sel] = maxc
        
             
        return  
    
    def shift_conncomp(self,cim):
        sel = np.logical_and(cim[0] > 0,cim[0] < WATER_VALUE)
        offset = np.max(cim[0][sel])
        sel = np.logical_and(cim[1] > 0,cim[1] < WATER_VALUE)
        cim[1][sel] = cim[1][sel] + offset
        return [cim[0],cim[1]]
            
    def stitch_pair(self,imin1,imin2,size1,size2,outname=''):
        print('stitch_pair')
        delta = size1['lon']['delta']
        nlat1 = int(size1['lat']['size'])
        lat1 = (size1['lat']['val'])
        nlon1 = int(size1['lon']['size'])
        lon1 = (size1['lon']['val'])
        nlat2 = int(size2['lat']['size'])
        lat2 = (size2['lat']['val'])
        nlon2 = int(size2['lon']['size'])
        lon2 = (size2['lon']['val'])
        bands = imin1[0].shape[1]
        imamp = imin1[0][:,0,:]
        im = imin1[0][:,1,:]
        cim = imin1[1]
        pim = imin1[2]
        im1amp = imin2[0][:,0,:]
        im1 = imin2[0][:,1,:]
        cim1 = imin2[1]
        pim1 = imin2[2]
        #i1,2 j1,2 are the offsets of im and im1 w.r.t. large image 
        #total width of combined images
        if lon1 > lon2:
            width  = int(((lon1 - lon2) + nlon1*delta)/delta)
            j1 = int((lon1 - lon2)/delta)
            j2 = 0
        else:
            width  = int(((lon2 - lon1) + nlon2*delta)/delta)
            j1 = 0
            j2 = int((lon2 - lon1)/delta)
        #total length of combined images
        if lat1 > lat2:
            length = int(((lat1 - lat2) + nlat2*delta)/delta)
            i1 = 0
            i2 = int((lat1 - lat2)/delta)
        else:
            length = int(((lat2 - lat1) + nlat1*delta)/delta)
            i1 = int((lat2 - lat1)/delta)
            i2 = 0
        
        wmsk1 = self.crop_mask(size1,self._wmask,'dummy.out')
        wmsk2 = self.crop_mask(size2,self._wmask,'dummy.out')
        #compute the overlap
       
        over,overlap_mask = self.get_ovelap([im,im1],[wmsk1,wmsk2],length,width,[i1,i2],[j1,j2],False)
        if len(over[0]) == 0:
            return None,None,None,None
        #don't touch the zeros so use this mask
        nmask1 = np.nonzero(np.abs(im) < self._small)
        nmask2 = np.abs(im1) < self._small
        im[nmask1] = 0
        im1[nmask2] = 0
        pim[nmask1] = 0
        pim1[nmask2] = 0
        im[wmsk1 == -1] = 0
        im1[wmsk2 == -1] = 0
        imamp[wmsk1 == -1] = 0
        im1amp[wmsk2 == -1] = 0
        imamp[nmask1] = 0
        im1amp[nmask2] = 0
        pim[wmsk1 == -1] = 0
        pim1[wmsk2 == -1] = 0 
        #the above pixels have ccomp = 0, but so do the failed ones. to distinguish set the former to -1
        cim[nmask1] = WATER_VALUE
        cim1[nmask2] = WATER_VALUE
        cim[wmsk1 == -1] = WATER_VALUE
        cim1[wmsk2 == -1] = WATER_VALUE
        mask2 = np.nonzero(np.logical_not(nmask2))
        amask = np.nonzero(np.abs(im1amp) > self._small)
        #tim = np.zeros([length,2,width])
        tim = self.get_memmap(im.dtype,'w+',(length,bands,width),outname)
        if outname:
            tcim = self.get_memmap(np.uint8,'w+',(length,width),outname.replace('.geo','.conncomp.geo'))
            tpim = self.get_memmap(pim.dtype,'w+',(length,width),self._cor_name)

        else:
            tcim = self.get_memmap(np.uint8,'w+',(length,width),outname)
            tpim = self.get_memmap(pim.dtype,'w+',(length,width),outname)
        
        self.generate_extra_memmaps(width, length,outname)
        tcim[:,:] = WATER_VALUE
   
        #get phase offset between the two images
        imo = im[over[0] - i1,over[1] - j1]
        cimo = cim[over[0] - i1,over[1] - j1]
        imo1 = im1[over[0] - i2,over[1] - j2]
        cimo1 = cim1[over[0] - i2,over[1] - j2]
        
        #get the image that covers better with less conncomp
        for i in range(1,4):
            which,ucc1,ucc2,disc1,disc2 = self.ref_image(cimo, cimo1,i)
            if which >= 0:
                break
        if which < 0:
            print('Stitching failed')
            raise Exception
            
        ims = [im,im1]
        imos = [imo,imo1]
        cimos = [cimo,cimo1]
        cims = [cim,cim1]
        uccs = [ucc1,ucc2]
        discs = [disc1,disc2]
        if not self._stitch_only:
            self.adjust_conncomp(which,ims,cims,imos,cimos,uccs,discs)
        else:
            [cim,cim1] = self.shift_conncomp(cims)

        imamp,im1amp = self.fix_amps(imamp,im1amp)
        imoa = imamp[over[0] - i1,over[1] - j1]
        imo1a = im1amp[over[0] - i2,over[1] - j2]   
        #amplitude offset
        aoffset  = np.mean(imoa - imo1a)      
        
        #phsig image
        pimo = pim[over[0] - i1,over[1] - j1]
        pimo1 = pim1[over[0] - i2,over[1] - j2]
        #phsig offset
        poffset  = np.mean(pimo - pimo1)      
          
        
        tim[i1:i1+nlat1,1,j1:j1 + nlon1] = im
        tim[i2 + mask2[0],1,j2 + mask2[1]] = im1[mask2]
        tim[i1:i1+nlat1,0,j1:j1 + nlon1] = imamp
        tim[i2 + amask[0],0,j2 + amask[1]] = im1amp[amask] + aoffset        
        tcim[i1:i1+nlat1,j1:j1 + nlon1] = cim
        tcim[i2 + mask2[0],j2 + mask2[1]] = cim1[mask2]
        
        #reset the -1 cc to 0
        tcim[tcim == WATER_VALUE] = 0
        
        tpim[i1:i1+nlat1,j1:j1 + nlon1] = pim
        tpim[i2 + mask2[0],j2 + mask2[1]] = pim1[mask2] + poffset
        
        self.stitch_extra_images(i1,j1,nlat1,nlon1,i2,j2,mask2)
        
        size1['lat']['val'] = max(lat1,lat2)
        size1['lon']['val'] = min(lon1,lon2)
        size1['lat']['size'] = tim.shape[0]
        size1['lon']['size'] = tim.shape[2]
        return tim,tcim,tpim,size1

    def get_memmap_extra(self,im,mode):
        if not im.filename:
            fp = tempfile.NamedTemporaryFile()
            im.filename = fp.name
            fp.close()
        if im.scheme.lower() == 'bil':
            immap = np.memmap(im.filename, im.toNumpyDataType(), mode,
                            shape=(im.coord2.coordSize , im.bands, im.coord1.coordSize))
        elif im.scheme.lower() == 'bip':
            immap = np.memmap(im.filename, im.toNumpyDataType(), mode,
                              shape=(im.coord2.coordSize, im.coord1.coordSize, im.bands))
        elif im.scheme.lower() == 'bsq':
            immap = np.memmap(im.filename, im.toNumpyDataType(), mode,
                        shape=(im.bands, im.coord2.coordSize, im.coord1.coordSize))
        return np.squeeze(immap)  
    
    def generate_image(self,name,width,height):
        im = Image()
        im.bands = self._image_info[name]['bands']
        im.scheme = self._image_info[name]['scheme']
        im.dataType = self._image_info[name]['data_type']
        im.coord2.coordSize = height
        im.coord1.coordSize = width
        return im
    
    def generate_extra_memmaps(self,width,height,outname):
        self._extra_prds_out = []
        for name in self._extra_prd_names:
            im = self.generate_image(name, width, height)
            if outname:
                im.filename = name
            else:
                im.filename = outname
            self._extra_prds_out.append(self.get_memmap_extra(im,'w+'))
        
    def load_extra_images(self,ddir):
        ret = []
        for name in self._extra_prd_names:
            im1 = get_image(os.path.join(ddir,name) + '.xml')
            im1.filename = os.path.join(ddir,im1.filename.replace('merged/',''))
            if name not in self._image_info:                
                self._image_info[name] = {'scheme':im1.scheme,
                                          'data_type':im1.dataType,
                                          'bands':im1.bands}
            mm1 = self.get_memmap_extra(im1,'c')
            ret.append(mm1)
        return ret
    
    def stitch_extra_images(self,i1,j1,nlat1,nlon1,i2,j2,mask2):
        for i in range(len(self._extra_prds_in1)):
            im1 = self._extra_prds_in1[i]
            im2 = self._extra_prds_in2[i]
            name = self._extra_prd_names[i]
            if self._image_info[name]['bands'] == 1:
                self._extra_prds_out[i][i1:i1+nlat1,j1:j1+nlon1] = im1
                self._extra_prds_out[i][i2+mask2[0],j2+mask2[1]] = im2[mask2]
            else:
                for ii in range(self._image_info[name]['bands']):
                    if self._image_info[name]['scheme'].lower() == 'bil':
                        self._extra_prds_out[i][i1:i1+nlat1,ii,j1:j1+nlon1] = im1[:,ii,:]
                        self._extra_prds_out[i][i2+mask2[0],ii,j2+mask2[1]] = im2[mask2[0],ii,mask2[1]]
                    elif self._image_info[name]['scheme'].lower() == 'bip':                    
                        self._extra_prds_out[i][i1:i1+nlat1,j1:j1+nlon1,ii] = im1[:,:,ii]
                        self._extra_prds_out[i][i2+mask2[0],j2+mask2[1],ii] = im2[mask2[0],mask2[1],ii]
                    elif self._image_info[name]['scheme'].lower() == 'bsq':                   
                        self._extra_prds_out[i][ii,i1:i1+nlat1,j1:j1+nlon1] = im1[ii,:,:]
                        self._extra_prds_out[i][ii,i2+mask2[0],j2+mask2[1]] = im2[ii,mask2[0],mask2[1]]
        
    def stitch_sequence(self,names,sizes,outname=''):
        print('stitch_sequence')
        bname = os.path.basename(names[0])
        im1 = get_image(names[0] + '.xml')
        shape = (sizes[0]['lat']['size'],im1.bands,sizes[0]['lon']['size'])
        mm1 = self.get_memmap(im1.toNumpyDataType(), 'c',shape,
                            os.path.join(os.path.dirname(names[0]),
                            os.path.basename(im1.filename)))
        
        cim1 = get_image(names[0].replace('.geo','.conncomp.geo') + '.xml')
        shape = (sizes[0]['lat']['size'],sizes[0]['lon']['size'])
        cmm1 = self.get_memmap(np.uint8, 'c',shape,
                            os.path.join(os.path.dirname(names[0]),
                            os.path.basename(cim1.filename)))
        
        pim1 = get_image(names[0].replace(bname,self._cor_name) + '.xml')
        shape = (sizes[0]['lat']['size'],sizes[0]['lon']['size'])
        pmm1 = self.get_memmap(pim1.toNumpyDataType(), 'c',shape,
                            os.path.join(os.path.dirname(names[0]),
                            os.path.basename(pim1.filename)))
        self._extra_prds_in1 = self.load_extra_images(os.path.dirname(names[0]))
        self.zero_n2pi_full(mm1[:,1,:]) 
        self.remove_small_cc(cmm1,mm1[:,1,:])
        size1 = sizes[0]
        #if there is only one image in the sequence then set is as the output product
        if len(names) == 1:
            self.generate_extra_memmaps(mm1.shape[2],mm1.shape[0],outname)
            for i in range(len(self._extra_prds_in1)):
                self._extra_prds_out[i] = self._extra_prds_in1[i].copy()
        for i in range(1,len(names)):
            im2 = get_image(names[i] + '.xml')
            shape = (sizes[i]['lat']['size'],im2.bands,sizes[i]['lon']['size'])
            mm2 = self.get_memmap(im2.toNumpyDataType(), 'c', shape,
                            os.path.join(os.path.dirname(names[i]),
                            os.path.basename(im2.filename)))
            
            cim2 = get_image(names[i].replace('.geo','.conncomp.geo') + '.xml')
            shape = (sizes[i]['lat']['size'],sizes[i]['lon']['size'])
            cmm2 = self.get_memmap(np.uint8, 'c',shape,
                            os.path.join(os.path.dirname(names[i]),
                            os.path.basename(cim2.filename)))
            pim2 = get_image(names[i].replace(bname,self._cor_name) + '.xml')
            shape = (sizes[i]['lat']['size'],sizes[i]['lon']['size'])
            pmm2 = self.get_memmap(pim2.toNumpyDataType(), 'c',shape,
                                os.path.join(os.path.dirname(names[i]),
                                os.path.basename(pim2.filename)))
            self._extra_prds_in2 = self.load_extra_images(os.path.dirname(names[i]))
            self.zero_n2pi_full(mm2[:,1,:])
            self.remove_small_cc(cmm2,mm2[:,1,:])
            if outname and i == len(names) - 1:
                fname = outname
            else:
                fname= ''     
            #get the new image and the new lat lon
            mm1,cmm1,pmm1,size1 = self.stitch_pair([mm1,cmm1,pmm1], [mm2,cmm2,pmm2], size1, sizes[i],fname)
            self._extra_prds_in1 = []
            for p in self._extra_prds_out:
                self._extra_prds_in1.append(p.copy())
            if mm1 is None:
                return None,None,None,None
            
        return mm1,cmm1,pmm1,size1
    
    def zero_products(self,cc,cor):
        mask = np.nonzero(np.logical_or(cc == 0,cc == -1))
        cor[mask] = 0
        for i in range(len(self._extra_prds_in1)):
            name = self._extra_prd_names[i]
            if self._image_info[name]['bands'] == 1:
                self._extra_prds_out[i][mask] = 0
            else:
                for ii in range(self._image_info[name]['bands']):
                    if self._image_info[name]['scheme'].lower() == 'bil':
                        self._extra_prds_out[i][mask[0],ii,mask[1]] = 0
                    elif self._image_info[name]['scheme'].lower() == 'bip':                    
                        self._extra_prds_out[i][mask[0],mask[1],ii] = 0
                    elif self._image_info[name]['scheme'].lower() == 'bsq':                   
                        self._extra_prds_out[i][ii,mask[0],mask[1]] = 0
        return

    def check_overlap(self,blats,elats):
        ret = True
        for blat,elat in zip(blats,elats):
            indx = np.lexsort((blat,elat))
            sblat = blat[indx]
            selat = elat[indx]
            if np.any(selat[:-1] - sblat[1:] < 0):
                ret = False
                break
        return ret
            
            
        
        
        
    def stitch(self,args):
        while True:#just a trick to avoid a lot of nested if statements
            if 'extra_products' in args: 
                self._extra_prd_names = args['extra_products']
            if 'stitch_only' in args:
                self._stitch_only = args['stitch_only'] 
            names,sizes = self.arrange_frames(args['filenames'])
            if names is None:
                print('No contiguous frames')
                raise Exception
            self.create_mask(sizes)
            if args['direction'] == 'along':
                nnames = []
                ssizes = []
                for i in range(len(names[0])):
                    nm = []
                    sz = []
                    for j in range(len(names)):
                        nm.append(names[j][i])
                        sz.append(sizes[j][i])
                    nnames.append(nm)
                    ssizes.append(sz)
                names = nnames
                sizes = ssizes
            elif args['direction'] != 'across':
                print('Stitch direction either across or along. Entered',args['direction'])
                raise Exception
            #im1,cm1,size1 = self.stitch_sequence(names[1], sizes[1])
            #if there is only one subswath and the direction is along than 
            #the stich _equence will already stitch all the ifgs so give
            #the outname
            if  len(names) == 1:
                outname = args['outname']
            else:
                outname = ''
            im1,cm1,pm1,size1 = self.stitch_sequence(names[0], sizes[0],outname)
            #NOTE: cannot use the self._extra_prds_in1 since it gets overwritten
            #in stitch_sequence
            extra_prds_in1 = []
            for p in self._extra_prds_out:
                extra_prds_in1.append(p.copy())
            if im1 is None:
                print('Stitching failed')
                break
            
            i = 1
            for name,size in zip(names[1:],sizes[1:]):
                im2,cm2,pm2,size2 = self.stitch_sequence(name, size)
                if im2 is None:
                    print('Stitching failed')
                    break 
                self._extra_prds_in1 = extra_prds_in1
                self._extra_prds_in2 = []
                for p in self._extra_prds_out:
                    self._extra_prds_in2.append(p.copy())
                
                if i == len(names) - 1:
                    outname = args['outname']
                im1,cm1,pm1,size1 = self.stitch_pair([im1,cm1,pm1],[im2,cm2,pm2],size1,size2,outname) 
                if im1 is None:
                    print('Stitching failed')
                    break
                extra_prds_in1 = []
                for p in self._extra_prds_out:
                    extra_prds_in1.append(p.copy())
               
                i += 1
            #zero where ccomp == 0
            sel = np.nonzero(cm1 == 0)
            #zero the amp
            im1[sel[0],0,sel[1]] = 0
            if len(names[0]) == 1:
                #the mmap has not been generated for te final product su just dump im1
                im1.tofile(outname)
            self.zero_products(cm1,pm1)
            self.save_image(os.path.join(os.path.dirname(names[0][0]),outname),outname,size1)
            ccname = outname.replace('.geo','.conncomp.geo')
            self.save_image(os.path.join(os.path.dirname(names[0][0]),ccname),ccname,size1)
            
            self.save_image(os.path.join(os.path.dirname(names[0][0]),self._cor_name),self._cor_name,size1)
            for name in self._extra_prd_names:
                self.save_image(os.path.join(os.path.dirname(names[0][0]),name),name,size1)

            #if reaches the bottom everything went ok so break
            break
    
    def two_stage_unwrap(self, unwrappedIntFilename, ccFile,unwrapped2StageFilename = None, unwrapper_2stage_name = None, solver_2stage = None):
        
        if unwrapper_2stage_name is None:
            unwrapper_2stage_name = 'REDARC0'
    
        if solver_2stage is None:
            # If unwrapper_2state_name is MCF then solver is ignored
            # and relaxIV MCF solver is used by default
            solver_2stage = 'pulp'
        if unwrapped2StageFilename is None:
            unwrapped2StageFilename = unwrappedIntFilename.replace('.unw','_2stage.unw')
        print('Unwrap 2 Stage Settings:')
        print('Name: %s'%unwrapper_2stage_name)
        print('Solver: %s'%solver_2stage)
    
    
        inpFile = os.path.join(unwrappedIntFilename)
        outFile = os.path.join(unwrapped2StageFilename)
    
        # Hand over to 2Stage unwrap
        unw = UnwrapComponents()
        unw.setInpFile(inpFile)
        unw.setConnCompFile(ccFile)
        unw.setOutFile(outFile)
        unw.setSolver(solver_2stage)
        unw.setRedArcs(unwrapper_2stage_name)
        unw.unwrapComponents()
#fname is the name of the json file with keys
#"outname":"output filename", #normally something like filt_topophase.unw.geo
#"filenames":[[["run_1_1/merged/filt_topophase.unw.geo",
               #"run_1_2/merged/filt_topophase.unw.geo",
               #"run_1_3/merged/filt_topophase.unw.geo"],
               #["run_2_1/merged/filt_topophase.unw.geo",
               #"run_2_2/merged/filt_topophase.unw.geo",
               #"run_2_3/merged/filt_topophase.unw.geo"]]]
### NOTE: each row the names must be arranged by subswath increasing number

def main(fname):
    inps = json.load(open(fname))
    st = IfgStitcher()
    st.stitch(inps)
    #st.two_stage_unwrap('filt_topophase.unw.geo','filt_topophase.unw.conncomp.geo')
    pass
         
if __name__ == '__main__':
    sys.exit(main(sys.argv[1]))
