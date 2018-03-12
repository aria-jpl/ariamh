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
from .FeaturesExtractor import FeaturesExtractor
class ExpFeaturesExtractor(FeaturesExtractor):
    def __init__(self, url, productName, coThr=None):
        super().__init__(url, productName, coThr)
        self._newSize = [150,200]#length and width
        self._numCoher = 2
        self._numGrad = 2
        self._numCComp = 2
        self._imThr = .3
        self._coverageThresh = [.5,.9]

    #If few pixels don't bother
    def coherenceDist(self,coherin,mask):
        coher = coherin[mask]
        if coher.size < self._imThr*coherin.size:
            ret = np.zeros(self._numCoher)
        else:
            ret = np.array([np.mean(coher[:]),np.std(coher[:])])
        return ret
    #If few pixels don't bother
    def gradientDist(self,grdin,mask):
        grd = grdin[mask]
        if grd.size < self._imThr*grdin.size:
            ret = np.zeros(self._numGrad)
        else:
            ret = np.array([np.mean(grd[:]),np.std(grd[:])])
        return ret
    
    #returns the size of each tile so that the image is resampled to _newSize
    def getTiling(self,dims):
        tilel = np.ones(self._newSize[0],dtype=np.int)*dims[0]//self._newSize[0]
        tilew = np.ones(self._newSize[1],dtype=np.int)*dims[1]//self._newSize[1]
        #there are pixels left so add one per tile until none is left
        tilel[0:(dims[0]%self._newSize[0])] += 1
        tilew[0:(dims[1]%self._newSize[1])] += 1
        return np.cumsum(tilel),np.cumsum(tilew)
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
        tilel,tilew = self.getTiling(self._imgMap['coher']['img'].shape)
        ret['outputs'] = {}

        for coTh in self._coThr:
            cdim = np.zeros([self._newSize[0],self._newSize[1],self._numCoher])
            grdim = np.zeros([self._newSize[0],self._newSize[1],self._numGrad])
            topoim = np.zeros([self._newSize[0],self._newSize[1]])
            connim = np.zeros([self._newSize[0],self._newSize[1],self._numCComp])
            resim = np.zeros([self._newSize[0],self._newSize[1]])
            featDict = {}
            self.goodRegion(coTh)
            lprev = 0
            #residues needs to remove first row and column
            extral = 1           
            for i,l in enumerate(tilel):
                wprev = 0
                extraw = 1
                sell = np.arange(lprev,l)
                for j,w in enumerate(tilew):
                    mask = self._masks['mask'][lprev:l,wprev:w]
                    cdim[i,j,:] = self.coherenceDist(self._imgMap['coher']['img'][lprev:l,wprev:w],mask)
                    grdim[i,j,:] = self.gradientDist(self.computeGradient(self._imgMap['phgeo']['img'][lprev:l,wprev:w],(lprev,l,wprev,w)),mask)
                    topoim[i,j] = self.topoCorr(self._imgMap['dem']['img'][lprev:l,wprev:w],self._imgMap['phgeo']['img'][lprev:l,wprev:w],mask)
                    connim[i,j,:] = self.connComp(self._imgMap['ccomp']['img'][lprev:l,wprev:w],mask)                     
                    res = self.residues(resid[lprev-(extral+1)%2:l-1,wprev-(extraw+1)%2:w-1],self._masks['mask'][lprev+extral:l,wprev+extraw:w])
                    resim[i,j] = res[0] + res[1]
                    wprev = w
                    extraw = 0
                lprev = l
                extral = 0
            featDict['coherenceDist'] = cdim
            featDict['gradientDist'] = grdim
            featDict['topoCorr'] = topoim
            featDict['connComp'] = connim
            featDict['residues'] = resim
            ret['outputs'][str(int(coTh*10))] = featDict
        return ret

