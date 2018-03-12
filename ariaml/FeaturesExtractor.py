 #!/usr/bin/env python3
from utils.queryBuilder import buildQuery, postQuery
from utils.UrlUtils import UrlUtils
import os
import sys
import json
import isce
from math import  floor, ceil
from isceobj.Image import createDemImage, createImage
import numpy as np
import matplotlib
from matplotlib import pyplot as plt
from scipy.ndimage.morphology import binary_dilation
from skimage.feature import canny
from skimage.segmentation import find_boundaries
from scipy.stats import pearsonr
from scipy import r_, degrees
import tempfile
class FeaturesExtractor:
    def __init__(self, url, productName, coThr=None):
        self._eps = 10**-20
        self._url = url
        self._productList = ["filt_topophase.unw.conncomp.geo",
                             "filt_topophase.unw.geo",
                             "filt_topophase.unw",
                             "filt_topophase.unw.conncomp",
                             "topophase.cor.geo",
                             "filt_topophase.unw.conncomp.geo.xml",
                             "filt_topophase.unw.geo.xml",
                             "filt_topophase.unw.xml",
                             "filt_topophase.unw.conncomp.xml",
                             "topophase.cor.geo.xml"
                             ]
        self._demName = 'dem.dem'
        self._wbdName = 'wbd.msk'
        self._cropPrefix = 'cropped_'
        self._imgMap = {
                        'phgeo':{'name':'filt_topophase.unw.geo.xml','img':None,'band':1},
                        'phase':{'name':'filt_topophase.unw.xml','img':None,'band':1},
                        'pcomp':{'name':'filt_topophase.unw.conncomp.xml','img':None,'band':0},
                        'coher':{'name':'topophase.cor.geo.xml','img':None,'band':1},
                        'ccomp':{'name':'filt_topophase.unw.conncomp.geo.xml','img':None,'band':0},
                        'dem':{'name':self._cropPrefix + self._demName + '.xml','img':None,'band':0},
                        'wbd':{'name':self._cropPrefix + self._wbdName + '.xml','img':None,'band':0}
                        }
        self._masks = {}
        if coThr is None:
            coThr = [.2,.4,.6]
        elif not isinstance(coThr,list):
            #assume is single number
            coThr = [coThr]
            
        self._coThr = coThr
        self._coverageThresh = [.5,.75,.9,.95]
        self._edgeKernelw = [5,15]
        self._slopeBins = r_[np.linspace(0,30,6+1),np.linspace(45,90,3+1)]
        self._productName = productName
        self._maxConnComp = 32
    def residues(self,resid,mask):
        mresid = resid[mask]
        if mresid.size == 0:
            #the sum has to be one and small number is good so make it as big as possible
            ret = [.5,.5]
        else:
            ret =  [np.nonzero(mresid == -1)[0].shape[0]/mresid.shape[0],np.nonzero(mresid == 1)[0].shape[0]/mresid.shape[0]]
        
        return ret
    def computeResidues(self,phase):
        a = phase[:,:]/(2*np.pi)
        resid = np.zeros(a[1:,1:].shape,dtype = np.int8)
        mats = [a[:-1,:-1],a[:-1,1:],a[1:,1:],a[1:,:-1]]
        nmats = len(mats)
        for i in range(nmats):
            resid  =  resid + np.round(mats[(i+1)%nmats] - mats[i]).astype(np.int8)
        return resid
    #bin width = .1 from the min value to 1.
    def coherenceDist(self,coherin,mask):
        coher = coherin[mask]
        mv = int(np.min(coher[:])*10)/10
        nbins = int(round((1-mv)/.1))
        hist =  np.histogram(coher[:],nbins)[0]
        return hist/np.cumsum(hist)[-1]
   
    def rms(self,phase,mask):
        return np.std(phase[mask])
    
    def getData(self):
        uu = UrlUtils()
        for pr in self._productList:
            command = 'curl -k -f -u' + uu.dav_u + ':' + uu.dav_p + ' -O ' + os.path.join(self._url, pr)
            print(command)
            if os.system(command) != 0:
                command = 'curl -k -f -u' + uu.dav_u + ':' + uu.dav_p + ' -O ' + os.path.join(self._url,"merged",pr)
                print(command)
                if os.system(command) != 0:
                    print("Failed to find: {0}".format(pr))

    def computeGradient(self,image,sel):
        grd = np.gradient(image)
        ms = np.invert(binary_dilation(self._masks['border'][sel[0]:sel[1],sel[2]:sel[3]]))
        return np.sqrt(grd[0]*grd[0] + grd[1]*grd[1])*ms
    
    #compute distribution of gradient in pi/8 intervals plus whatever left from pi to 2pi
    def gradientDist(self,grd,mask):
        hist = np.histogram(grd[mask],np.append(np.pi/8*np.arange(9),2*np.pi))[0]
        if np.cumsum(hist)[-1] > 0:
            div = np.cumsum(hist)[-1]
        else:
            div = 1.
        return hist/div
    
    ##
    #@param geoname = str name of the reference geocoded image
    #@param names = list names of the dem and wbd mask
    def getDemAndWbd(self, geoname, names):
        image = createDemImage()
        image.load(geoname)
        latmax = ceil(image.coord2.coordStart)
        latmin = floor(image.coord2.coordStart + image.coord2.coordSize * image.coord2.coordDelta)
        lonmin = floor(image.coord1.coordStart)
        lonmax = ceil(image.coord1.coordStart + image.coord1.coordSize * image.coord1.coordDelta)
        bbox = ''.join(str([latmin, latmax, lonmin, lonmax]).split())
        command = 'stitcher.py stitcher.xml stitcher.demStitcher.bbox=' + bbox \
                + ' stitcher.demStitcher.outputfile=' + names[0] + " > sticher.log"
        print(command)
        os.system(command)
        command = 'wbdStitcher.py swbdStitcher.xml wbdstitcher.wbdstitcher.bbox=' + bbox \
                + ' wbdstitcher.wbdstitcher.outputfile=' + names[1] + " > wdbStitcher.log"
        print(command)
        os.system(command)
    
    ##
    #@param geoname = str name of the reference geocoded image
    #@param innames = list names of the dem and wbd mask 
    #@oaram outname = list names of the cropped dem and wbd mask
    def cropDemAndWbd(self,geoname,innames,outnames):
        self.cropImage(geoname,innames[0],outnames[0],'dem')
        self.cropImage(geoname,innames[1],outnames[1],'wbd')
    def resample(self,ratio,im):
        if ratio > 1:#upsample           
            #make it an int
            ratio = int(round(ratio))
            newIm = np.repeat(np.repeat(im,ratio,0),ratio,1)

        else:#downsample
            ratio = int(round(1/float(ratio)))
            newIm = im[::ratio,::ratio]
        return newIm
        
    def cropImage(self,geoname,inname,outname,which):
        im1 = createImage()
        im1.load(geoname)
        if which == 'dem':
            im2 = createDemImage()
        elif which == 'wbd':
            im2 = createImage()
        else:
            raise Exception('Unrecognized image type')
        im2.load(inname)
        
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
         #if it the resolutions are different the ration will normally  be either twice or half
        #or some integer (or inverse integer ratio)
        if ((londelta2/londelta1) > 1.5 or (londelta2/londelta1) < .8):
            #make sure that is close to an int
            if(abs(londelta2/londelta1 - round(londelta2/londelta1)) < .001 
               or  abs(londelta1/londelta2 - round(londelta1/londelta2)) < .001):
                imIn = self.resample((londelta2/londelta1),imIn)
            else:
                raise Exception('Cannot resample DEM and water mask to data grid')
                
        #create mmap of goename size but dem data type
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
        
   
    def localizeData(self):
        self.getData()
        geoname = ''
        for pr in self._productList:
            if pr.endswith('.geo.xml'):
                geoname = pr
                break
        if not geoname:
            print('Cannot find any geocoded product')
            raise Exception
        self.getDemAndWbd(geoname, [self._demName,self._wbdName])

    def loadImage(self, imgxml,band=None):
        if imgxml == self._imgMap['dem']['name']:
            img = createDemImage()
        else:
            img = createImage()
        img.load(imgxml)
        img.filename = os.path.basename(img.filename)
        return img.memMap(band=band)
    
    def loadImages(self):
        for k,v in self._imgMap.items():
            v['img'] = self.loadImage(v['name'],v['band'])
            
    def goodRegion(self,coThr):
        coher = self._imgMap['coher']['img']
        phgeo = self._imgMap['phgeo']['img']
        wbd = self._imgMap['wbd']['img']
        ccomp = self._imgMap['ccomp']['img']
        #don't like to test for == 0 with floats
        self._masks['border'] = (np.abs(coher) < self._eps) & (np.abs(phgeo) < self._eps)
        self._masks['water'] = (wbd == -1)
        self._masks['coherence'] = (coher < coThr)
        self._masks['mask'] =  (self._masks['border']==0)  & (ccomp != 0) &\
                         (self._masks['water']==0)  & (self._masks['coherence']==0)
                         
     
    def topoCorr(self,dem,ph,mask):
        if dem[mask].size == 0:
            ret = 0
        else:
            try:
                ret = pearsonr(dem[mask],ph[mask])[0]
            except RuntimeWarning:
                ret = 0
        return ret    
        
    def connComp(self,connin,mask):
        conn = connin[mask]
        vconn = np.unique(conn)
        if vconn.size > 0:
            percent = np.array([np.mean(conn==j) for j in vconn])
            percent.sort()
            percent = percent[::-1]
            cpercent = np.cumsum(percent)
            feat = []
            for th in self._coverageThresh:
                feat.append(np.nonzero(cpercent >= th)[0][0] + 1)
        else:
            feat = np.zeros(len(self._coverageThresh))
        return np.array(feat)/self._maxConnComp
       
    def nodata(self,img,nodata_value=None):
        mask = (np.isnan(img) | np.isinf(img))
        if nodata_value is not None:
            mask |= (img==nodata_value)        
        return mask    
    
    def slope(self,gridxy):
        dx, dy = np.gradient(gridxy)
        slope = 0.5*np.pi - np.arctan(np.hypot(dx,dy))
        return slope    
    
    def edgeStrength(self,phgeo,mask,kernelw):
        edges  = canny(phgeo,sigma=kernelw)    
        bounds = find_boundaries(edges,mode='thick')
        slopedeg = degrees(self.slope(phgeo))
        histv,_ = np.histogram(abs(slopedeg[(bounds & mask)]),bins=self._slopeBins)
        return histv
    
    def computeEdgeStrength(self,ph,mask):
        feats = []
        for kw in self._edgeKernelw:
            edge_kw = self.edgeStrength(ph,mask,kw)
            feats = r_[feats,edge_kw]
        return feats
    
    def extractFeatures(self):
        from datetime import datetime as time
        self.localizeData()
        self.cropDemAndWbd(self._imgMap['phgeo']['name'], [self._demName+'.xml',self._wbdName+'.xml'],
                            [self._imgMap['dem']['name'].replace('.xml',''),self._imgMap['wbd']['name'].replace('.xml','')])
        self.loadImages()
        resid = self.computeResidues(self._imgMap['phgeo']['img'])
        ret = {}
        #since the label might not be assigned when computing the features,
        #add the product name so one can do a quick retrieval of the label if needed by
        #using a query string. Can use the queryBuilder and pass it as tag
        ret['productName'] = self._productName
        ret['coverageThresh']  = self._coverageThresh
        ret['edgeKernelw']  = self._edgeKernelw
        ret['slopeBins']  = self._slopeBins.tolist()
        ret['maxConnComp']  = self._maxConnComp
        for coTh in self._coThr:
            featDict = {}
            self.goodRegion(coTh)
            cd = self.coherenceDist(self._imgMap['coher']['img'], self._masks['mask'])
            sel = [0,self._masks['mask'].shape[0],0,self._masks['mask'].shape[1]]
            grd = self.gradientDist(self.computeGradient(self._imgMap['phgeo']['img'],sel),self._masks['mask'])
            topo = self.topoCorr(self._imgMap['dem']['img'],self._imgMap['phgeo']['img'],self._masks['mask'])
            conn = self.connComp(self._imgMap['ccomp']['img'],self._masks['mask']) 
            feats = self.computeEdgeStrength(self._imgMap['phgeo']['img'], self._masks['mask'])
            #for residues the matrix is missing one element per direction
            res = self.residues(resid, self._masks['mask'][1:,1:])
            rms = float(self.rms(self._imgMap['phgeo']['img'],self._masks['mask']))
            featDict['coherenceDist'] = cd.tolist()
            featDict['gradientDist'] = grd.tolist()
            featDict['topoCorr'] = topo
            featDict['connComp'] = conn.tolist()
            featDict['edgeStrength'] = feats.tolist()
            featDict['residues'] = res
            featDict['rms'] = rms
            ret[str(int(coTh*10))] = featDict
        return ret

