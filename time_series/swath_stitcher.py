from matplotlib import pyplot as plt
import h5py
import numpy as np
from scipy.ndimage.morphology import binary_dilation
from scipy.ndimage import generate_binary_structure

class SwathStitcher:
    def __init__(self):
        #list of file pointers to h5. when loading in load_ts the names need to be in order 
        #west to east
        self._fps = []
        #output file pointer
        self._fpo = None
        #contains the indeces of each timeseries of the common dates
        self._dates_indx = []
        #start lat and lon of each ts w.r.t. the merged image 
        self._offsets = []
        #the merged image size
        self._size = []
        self._niter = 100
        #the subswath 1,2,3 might go right to left or letf to right depending
        #on the orbit direction, ascending or discending
        self._order = ''#'inc' or 'dec'. automatically computed
    
    def set_order(self):
        valid0 = np.nonzero(self.get_mask(self._fps[0]['recons'][0,:,:],np.nan))[1] 
        valid1 = np.nonzero(self.get_mask(self._fps[1]['recons'][0,:,:],np.nan))[1] 
        minlon0 = self._fps[0]['lon'][np.min(valid0)]
        minlon1 = self._fps[1]['lon'][np.min(valid1)]
        if minlon0 < minlon1:
            self._order = 'inc'
        else:
            self._order = 'dec'

    #find the indeces in dates1 where values of dates2 are found
    def get_date_indx(self,dates1,dates2):
        ret = []
        for d2 in dates2:
            ret.append(np.nonzero(np.abs(dates1 - d2) < .01)[0][0])
        return np.array(ret,np.int).tolist()
    
    def get_common_dates(self):
        inters = self._fps[0]['dates']
        for f in self._fps[1:]:
            inters = np.intersect1d(f['dates'],inters)
        ret = []
        for f in self._fps:
            ret.append(self.get_date_indx(f['dates'],inters))
        return ret
        
    def get_overlap(self,im1,im2,val):
        msk1 = self.get_mask(im1,val)
        msk2 = self.get_mask(im2,val)
        mask1 = np.zeros(msk1.shape,np.int8)
        mask2 = np.zeros(msk2.shape,np.int8)
        mask1[msk1] = 1
        mask2[msk2] = 1
        return (mask1 + mask2 == 2)
    
    #return True for all the values that ARE NOT val 
    def get_mask(self,im1,val):
        if np.isnan(val):
            msk1 = np.logical_not(np.isnan(im1))
        else:
            msk1 = np.logical_not(im1 == val)
        return msk1
    
    def remove_offset(self,im1,im2,msk):
        im11 = im1.copy()
        return im11 - np.median(im1[msk] - im2[msk])
     
    def load_ts(self,fnames):
        for f in fnames:
            self._fps.append(h5py.File(f))
        self._dates_indx = self.get_common_dates()
    
    def create_output(self,fname):
        self._fpo = h5py.File(fname,'w')
    
    def set_coords(self):
        minlat,maxlat,dlat,minlon,maxlon,dlon = self.get_common_bbox()
        lat = maxlat - np.arange(int(round((maxlat-minlat)/abs(dlat))) + 1)*abs(dlat)
        lon = minlon + np.arange(int(round((maxlon-minlon)/dlon)) + 1)*dlon
        self._fpo.create_dataset('lat',data = lat)
        self._fpo.create_dataset('lon',data = lon)
        
    def set_bperp(self):
        self._fpo.create_dataset('bperp',data = self._fps[0]['bperp'][:])

    def set_dates(self):
        self._fpo.create_dataset('dates',data = self._fps[0]['dates'][self._dates_indx[0]])
       
    def set_mName(self):
        self._fpo.create_dataset('mName',data = self._fps[0]['mName'][:])

    def set_masterind(self):
        self._fpo.create_dataset('masterind',data = 
                                 np.array([f['masterind'][()] for f in self._fps],np.int8))

    def set_regF(self):
        self._fpo.create_dataset('regF',data = self._fps[0]['regF'][:])

    def set_time(self):
        self._fpo.create_dataset('time',data = self._fps[0]['time'][self._dates_indx[0]])
    
    def set_tims(self):
        self._fpo.create_dataset('tims',data = self._fps[0]['tims'][self._dates_indx[0]])
    
    def set_cmask(self):
        offsets = self.offsets
        cmask = np.zeros(self.size)
        for i,f in enumerate(self._fps):
            #find all the non zero value in the mask
            sel = np.nonzero(f['cmask'][:] > 0)
            #reset the origine based on the image offset
            sel = (sel[0] + offsets[i][0], sel[1] + offsets[i][1])
            #we are doing or so it's ok if some get overwritten
            cmask[sel] = 1
        self._fpo.create_dataset('cmask',data = cmask)    
    
    def set_ifgcnt(self):
        ifgcnt = np.zeros(self.size,np.int32)
        offsets = self.offsets
        for i in range(len(self._fps)-1):
            ifgc1 = np.zeros(self.size,np.int32)
            shape = self._fps[i]['ifgcnt'].shape
            ifgc1[offsets[i][0]:offsets[i][0] + shape[0],offsets[i][1]:offsets[i][1] + shape[1]] = self._fps[i]['ifgcnt']
            ifgc2 = np.zeros(self.size,np.int32)
            shape = self._fps[i+1]['ifgcnt'].shape
            ifgc2[offsets[i+1][0]:offsets[i+1][0] + shape[0],offsets[i+1][1]:offsets[i+1][1] + shape[1]] = self._fps[i+1]['ifgcnt']
            #only for the first ime include both igfs, otherwise just the second
            if i == 0:
                msk = self.get_mask(ifgc1,0)
                ifgcnt[msk] = ifgc1[msk]
            msk = self.get_mask(ifgc2,0)
            ifgcnt[msk] = ifgc2[msk]    
            overlap = self.get_overlap(ifgc1,ifgc2,0)
            shape = list(ifgcnt.shape)
            shape.append(2)
            both = np.zeros(shape,np.int32)
            both[:,:,0] = ifgc1
            both[:,:,1] = ifgc2
            #note that when indexing with overlap it becomes 2-d instead of 3-d
            ifgcnt[overlap] = np.min(both[overlap,:],1)
            
        self._fpo.create_dataset('ifgcnt',data = ifgcnt)
    
    def set_recons(self):
        self.adjust_stack('recons')
    
    def set_rawts(self):
        self.adjust_stack('rawts')

    def adjust_stack_alt(self,dname):
        import time
        #get shape of stack
        #only valid ifgs. this will become superflous once everything is fixed and they all have same dates
        nifgs = len(self._dates_indx[0])
        shape = list(self.size)
        shape.insert(0,nifgs)
        #create data on disk since it's too big to be kept in memory
        dtype = self._fps[0][dname].dtype
        dsetout = self._fpo.create_dataset(dname,shape,dtype=dtype)
        for j in range(nifgs):
            offsets = self.offsets
            ifgs = np.nan*np.ones(self.size,dtype)
            order = np.arange(len(self._fps))
            sign = 1
            if self._order == 'dec':#revert order
                order = order[::-1]
                sign = -1
            first_time = True
            for i in order[:-1]:
                start = time.time()
                #only for the first time include both igfs, otherwise just the second
                if first_time:
                    ifg1 = np.nan*np.ones(self.size,dtype)
                    shape = list(self._fps[i][dname].shape)[1:3]
                    ifg1[offsets[i][0]:offsets[i][0] + shape[0],offsets[i][1]:offsets[i][1] + shape[1]] = self._fps[i][dname][j,:,:]
                    msk1 = self.get_mask(ifg1,np.nan)
                    ifgs[msk1] = ifg1[msk1]
                    first_time = False
                ifg2 = np.nan*np.ones(self.size,dtype)
                shape = list(self._fps[i+sign*1][dname].shape)[1:3]
                ifg2[offsets[i+sign*1][0]:offsets[i+sign*1][0] + shape[0],offsets[i+sign*1][1]:offsets[i+sign*1][1] + shape[1]] = self._fps[i+sign*1][dname][j,:,:]                
                overlap = self.get_overlap(ifg1,ifg2,np.nan)
                '''
                PLAY with the interations to see which one works better
                '''
                doverlap = binary_dilation(overlap.astype(np.int8),structure=generate_binary_structure(2,2),iterations=self._niter)                
                msk2 = self.get_mask(ifg2,np.nan)
                if np.any(doverlap):
                    mean1 = np.mean(ifg1[np.logical_and(msk1,doverlap)])
                    mean2 = np.mean(ifg2[np.logical_and(msk2,doverlap)])
                    ifg2 = ifg2 - mean2 + mean1                
                ifgs[msk2] = ifg2[msk2]
                #ifgs[overlap] = (ifg1[overlap] + ifg2[overlap])/2.  
                #next round the second ifg is used as reference
                ifg1 = ifg2
                msk1 = msk2  
            dsetout[j,:,:] = ifgs
        return
    
    def adjust_stack(self,dname):
        import time
        #get shape of stack
        #only valid ifgs. this will become superflous once everything is fixed and they all have same dates
        nifgs = len(self._dates_indx[0])
        shape = list(self.size)
        shape.insert(0,nifgs)
        #create data on disk since it's too big to be kept in memory
        dtype = self._fps[0][dname].dtype
        dsetout = self._fpo.create_dataset(dname,shape,dtype=dtype)
        for j in range(nifgs):
            offsets = self.offsets
            ifgs = np.nan*np.ones(self.size,dtype)
            for i in range(len(self._fps)-1):
                start = time.time()
                #only for the first ime include both igfs, otherwise just the second
                if i == 0:
                    ifg1 = np.nan*np.ones(self.size,dtype)
                    shape = list(self._fps[i][dname].shape)[1:3]
                    ifg1[offsets[i][0]:offsets[i][0] + shape[0],offsets[i][1]:offsets[i][1] + shape[1]] = self._fps[i][dname][j,:,:]
                    msk = self.get_mask(ifg1,np.nan)
                    ifgs[msk] = ifg1[msk]
                ifg2 = np.nan*np.ones(self.size,dtype)
                shape = list(self._fps[i+1][dname].shape)[1:3]
                ifg2[offsets[i+1][0]:offsets[i+1][0] + shape[0],offsets[i+1][1]:offsets[i+1][1] + shape[1]] = self._fps[i+1][dname][j,:,:]                
                overlap = self.get_overlap(ifg1,ifg2,np.nan)
                msk = self.get_mask(ifg2,np.nan)
                if np.any(overlap):
                    ifg2 = ifg2 - np.median(ifg2[overlap] - ifg1[overlap])                
                ifgs[msk] = ifg2[msk]
                ifgs[overlap] = (ifg1[overlap] + ifg2[overlap])/2.  
                #next round the second ifg is used as reference
                ifg1 = ifg2  
            dsetout[j,:,:] = ifgs
        return
    
    def set_parms(self):
        import time
        ndim = self._fps[0]['parms'].shape[2]
        shape = list(self.size)
        shape.append(ndim)
        #create data on disk since it's too big to be kept in memory
        dtype = self._fps[0]['parms'].dtype
        dsetout = self._fpo.create_dataset('parms',shape,dtype=dtype)
        for j in range(ndim):
            offsets = self.offsets
            ifgs = np.nan*np.ones(self.size,dtype)
            for i in range(len(self._fps)-1):
                start = time.time()
                #only for the first ime include both igfs, otherwise just the second
                if i == 0:
                    ifg1 = np.nan*np.ones(self.size,dtype)
                    shape = list(self._fps[i]['parms'].shape)[0:2]
                    ifg1[offsets[i][0]:offsets[i][0] + shape[0],offsets[i][1]:offsets[i][1] + shape[1]] = self._fps[i]['parms'][:,:,j]
                    msk = self.get_mask(ifg1,np.nan)
                    ifgs[msk] = ifg1[msk]
                ifg2 = np.nan*np.ones(self.size,dtype)
                shape = list(self._fps[i+1]['parms'].shape)[0:2]
                ifg2[offsets[i+1][0]:offsets[i+1][0] + shape[0],offsets[i+1][1]:offsets[i+1][1] + shape[1]] = self._fps[i+1]['parms'][:,:,j]                
                overlap = self.get_overlap(ifg1,ifg2,np.nan)
                msk = self.get_mask(ifg2,np.nan)              
                ifgs[msk] = ifg2[msk]
                ifgs[overlap] = (ifg1[overlap] + ifg2[overlap])/2.  
                #next round the second ifg is used as reference
                ifg1 = ifg2  
            dsetout[:,:,j] = ifgs
        return
              
    def merge_datasets(self):
        self.set_order()
        self.set_coords()
        self.set_bperp()
        self.set_dates()
        self.set_mName()
        self.set_masterind()
        self.set_regF()
        self.set_time()
        self.set_tims()
        print("uncomment below")
        '''
        self.set_cmask()
        self.set_ifgcnt()
        '''
        self.set_recons()
        '''
        self.set_rawts()
        self.set_parms()
        '''
    def set_gamma(self):
        self._fpo.create_dataset('gamma',data = self._fps[0]['gamma'][()])

    def get_common_bbox(self):
        latmin = np.finfo(np.float).max
        latmax = -np.finfo(np.float).max
        lonmin = np.finfo(np.float).max
        lonmax = -np.finfo(np.float).max
        dlat = None
        dlon = None
        for f in self._fps:
            lat = f['lat']
            lon = f['lon']
            latmin = min(latmin,np.min(lat))
            lonmin = min(lonmin,np.min(lon))
            latmax = max(latmax,np.max(lat))
            lonmax = max(lonmax,np.max(lon))
            if dlat is None:
                dlat = np.mean(np.diff(lat))
                dlon = np.mean(np.diff(lon))
        
        return latmin,latmax,dlat,lonmin,lonmax,dlon
    
    
    def get_recons_ts(self):
        return self._fp1['recons'],self._fp2['recons']
    
    @property 
    def size(self):
        if not self._size:
            minlat,maxlat,dlat,minlon,maxlon,dlon = self.get_common_bbox()
            self._size = [int(round((maxlat-minlat)/abs(dlat))) + 1,int(round((maxlon-minlon)/abs(dlon))) + 1]
        return self._size
    
    @property
    def offsets(self):
        if not self._offsets:
            minlat,maxlat,dlat,minlon,maxlon,dlon = self.get_common_bbox()
            for f in self._fps:
                lat = f['lat']
                lon = f['lon']
                ilat = int(round((maxlat - np.max(lat))/abs(dlat)))
                ilon = int(abs(round((minlon - np.min(lon))/dlon)))
                self._offsets.append([ilat,ilon])
        return self._offsets
        