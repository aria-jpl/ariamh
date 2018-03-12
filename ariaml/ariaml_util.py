from __future__ import print_function
import sys, os
import numpy as np


pathexists = os.path.exists
pathjoin   = os.path.join
pathsplit  = os.path.split
expandvars = os.path.expandvars
expanduser = os.path.expanduser
abspath    = os.path.abspath
splitext   = os.path.splitext
normpath   = os.path.normpath

valid_tags  = ['UWE']
valid_users = ['EF','ZL','PM','PL','PR','SY','M','','?']
valid_labels = [0,1,2,3]

def url2pid(url):
    if url.endswith('/'):
        url = url[:-1]
    urlsplit = url.split('/')
    return (urlsplit[-2] + '_' + urlsplit[-1]).replace('__','_')

def strrems(s,rlist):
    for r in rlist:
        s = s.replace(r,'')
    return s

def tags2userlabs(tags,tagid='UWE',valid_users=valid_users,
                  valid_labels=valid_labels,
                  verbose=False):
    userlabs = []

    # convert labels to str if they're numerical
    valid_labels = list(map(str,valid_labels))    

    # sort strings by descending length so substring matching is correct
    sortstrlen = lambda l: l[np.argsort([len(s) for s in l])[::-1]]
    valid_labels = sortstrlen(np.asarray(valid_labels))
    valid_users = sortstrlen(np.asarray(valid_users))
    
    for tag in tags:
        if tagid in tag:
            label = None
            for l in valid_labels:
                if l in tag:
                    label = l
                    break
            if label is None:
                print('label not found in tag %s, skipping'%tag)
                sleep(sleeptime)
                continue
            user = strrems(tag,[tagid,label,'_','-']).strip()
            if user not in valid_users:
                print('invalid user %s for tag %s, skipping'%(user,tag))
                sleep(sleeptime)
                continue
            if user == '':
                user = '?'
            userlabs.append([user,label])
        elif verbose:
            print('skipping tag %s'%tag)
            
    return userlabs

def featurenames(featjson):
    featdat = loadjson(featjson)
    featnames = []
    featorder = featdat['feature_order']
    featdims = featdat['feature_dims']
    for j,fname in enumerate(featorder):
        for i in range(featdims[j]):
            featnames.append(fname+str(i))    
    return featnames

def plotfeatvec(featvec,featnames):
    import pylab as pl
    pl.plot(featvec,color='b')
    pl.xticks(range(feats.size),featnames,rotation=90)
    pl.xlim(-1,feats.size)
    
def stratifylist(imagelist,imagelabs,outdir='.'):
    from sklearn.cross_validation import train_test_split
    imagelist = array(imagelist)
    imagelabs = array(imagelabs)

    imageindex = arange(len(imagelist))
    trindex,teindex = train_test_split(imageindex, stratify=imagelabs,
                                       random_state=random_state,
                                       test_size=0.5)
    
    trpaths,trlab = imagelist[trindex],imagelabs[trindex].astype(str)
    tepaths,telab = imagelist[teindex],imagelabs[teindex].astype(str)

    troutf = pathjoin(outdir,'train_images.txt')
    teoutf = pathjoin(outdir,'test_images.txt')
    np.savetxt(troutf, c_[trpaths,trlab], fmt='%s %s')
    np.savetxt(teoutf, c_[tepaths,telab], fmt='%s %s')

def imrotate(img,ang,order=1):
    #from scipy.ndimage import rotate as _rotate # this assumes uint images!!!
    #imgout = _rotate(img.squeeze(),ang,order=order,output=img.dtype,prefilter=False)
    from skimage.transform import rotate as _rotate
    imgout = _rotate(img.squeeze(),ang,order=order,clip=1,preserve_range=True)
    return imgout

def rotatebbox(bboxxy, snap=0.25):
    '''
    computes rotation matrix minimizing *width* of GLT bounding box
    '''
    DEG2RAD = double(pi/180.0)
    rotmat = [[1.0,0.0],[0.0,1.0]]
    rotdeg = 0.0
    rotmin = inf
    rotxy  = bboxxy
    for r in arange(-90,91,snap):
        ar    = DEG2RAD*r
        cosar = cos(ar)
        sinar = sin(ar)
        rar   = [[cosar,-sinar], [sinar,cosar]]
        rxy   = dot(rar,bboxxy)
        xr,yr = extrema(rxy[0,:]),extrema(rxy[1,:])
        rdiff = abs(xr[1]-xr[0])
        # NOTE (BDB, 09/01/15): code below finds min size (not min width) bbox 
        #rdiff = amin([xr[1]-xr[0],yr[1]-yr[0]]) 
        if rdiff < rotmin:
            rotmin,rotdeg = rdiff,ar
            rotmat,rotxy  = rar,rxy

    return rotxy, rotmat, rotdeg/DEG2RAD

def rotatexy(x, y, theta, ox, oy):
    """
    Rotate arrays of coordinates x and y by theta radians about the
    point (ox, oy).
    """
    s, c = np.sin(theta), np.cos(theta)
    x, y = np.asarray(x) - ox, np.asarray(y) - oy
    return x * c - y * s + ox, x * s + y * c + oy

def zeromask(img,invert=0,axis=2):
    return ((img.sum(axis=axis)!=0) == invert)

def hsvmask(img,thr):
    '''
    filter out grayscale components of image, keeping only color values
    '''
    from skimage.color import rgb2hsv #, hsv2rgb
    hsv = rgb2hsv(img[:,:,:3])
    return (hsv[:,:,1]>thr)

#@functime
def impreprocess(rimg,**kwargs):
    """
    return a [nchan,ncols,nrows] image which has been rotated to fill its
    nonzero bounding box and filtered
    """

    rotate = kwargs.pop('rotate',False)
    scale = kwargs.pop('scale',1.0)
    crop = kwargs.pop('crop',True)
    interleave = kwargs.pop('interleave','bsq')
    dtype = kwargs.pop('dtype',rimg.dtype)
    outdims = kwargs.pop('outdims',[])
    hsvthr = kwargs.pop('hsvthr',0.0)
    maskval = kwargs.pop('maskval',0)
    eroderad = kwargs.pop('eroderad',15)
    
    if interleave == 'bsq':
        ncols,nrows,nchan = rimg.shape
    elif interleave == 'bip':
        nchan,ncols,nrows = rimg.shape
        rimg = rimg.transpose((1,2,0))
    elif interleave == 'bil':
        ncols,nchan,nrows = rimg.shape
        rimg = rimg.swapaxes(1,2)
    
    rchan = [0,1,2] if nchan in set([3,4]) else [0]

    if hsvthr != 0.0:
        rmask = hsvmask(rimg,hsvthr)
        for b in range(rimg.shape[2]):
            rimg[:,:,b] = rimg[:,:,b]*rmask

    if scale != 1.0:
        from skimage.transform import rescale
        rimg = rescale(rimg, scale, order=3, mode='constant',
                       cval=0, clip=False, preserve_range=True)            

    if rotate:
        #from skimage.segmentation import find_boundaries
        nonzerom = zeromask(rimg[:,:,[0]]!=maskval,True)
        #nonzeros = c_[where(find_boundaries(nonzerom,mode='inner'))].T
        nonzeros = c_[where(nonzerom)]
        nonzeros = nonzeros[chull(nonzeros)].T
        rxy,rmat,rdeg = rotatebbox(nonzeros,1)

        rimg = imrotate(rimg,rdeg)
        if len(rimg.shape) == 2:
            rimg = rimg[:,:,np.newaxis]

    nonzerom = zeromask(rimg[:,:,[0]]!=maskval,True)
    # from skimage.morphology import binary_erosion, disk
    # nonzerom = binary_erosion(nonzerom,disk(eroderad))
    # for b in range(rimg.shape[2]):
    #     rimg[:,:,b] = rimg[:,:,b]*nonzerom
    #nonzeros = c_[where(find_boundaries(nonzerom,mode='inner'))].T
    nonzeros = c_[where(nonzerom)].T
    #nonzeros = nonzeros[chull(nonzeros)].T
    minxy,maxxy = extrema(nonzeros,axis=1)
    cmin,cmax = max(0,int(minxy[0])-1),min(ncols,int(maxxy[0])+1)
    rmin,rmax = max(0,int(minxy[1])-1),min(nrows,int(maxxy[1])+1)

    rimg = rimg[cmin:cmax,rmin:rmax,rchan].astype(dtype)    
    if crop:
        outimg = rimg
    else:
        # embed image in a zero padded buffer of size outdims
        if len(outdims) == 0:
            outdims = [ncols,nrows,len(rchan)]
    
        outimg = zeros(outdims,dtype=dtype)
        romax,comax = rmax-rmin,cmax-cmin
        roshift = max(0,int((nrows-romax)/2))
        coshift = max(0,int((ncols-comax)/2))
        outimg[coshift:coshift+comax,roshift:roshift+romax,:] = rimg

    # zero nans, \pm infinity
    outimg[(outimg!=outimg)|(abs(outimg)==np.inf)] = 0
    return outimg.swapaxes(0,2)
        
def loadpng(pngf):
    from skimage.io import imread
    img = np.array(imread(pngf),dtype=np.uint8)
    return img.swapaxes(0,2)

def loadenvi(imgf,memmap=False):
    img = envi_open(imgf+'.hdr')
    if memmap:        
        imgmm = img.open_memmap()
        interleave = img.metadata['interleave']
        if interleave == 'bil':
            imv = imgmm.transpose((1,2,0))
        elif interleave == 'bip':
            imv = imgmm.swapaxes(1,2)
        elif interleave == 'bsq':
            imv = imgmm.swapaxes(0,2) 
    else:
        nbands = int(img.metadata['bands'])
        imv = np.array(img.read_bands(range(nbands)),dtype=np.float32)        
        imv = imv.transpose((2,0,1))
        
    return imv

def ingestmeta(metadir):
    from glob import glob
    from pandas import read_json, concat
    sdf = None
    for f in glob(pathjoin(metadir,'*.json')):
        sf = read_json(f,orient='index',typ='series')
        sf['name'] = splitext(pathsplit(f)[1])[0]
        sdf = sf if sdf is None else concat([sdf,sf])
    sdf.index = [sdf.name]    
    return sdf


def extrema(a,**kwargs):
    
    preserve_aspect = kwargs.pop('preserve_aspect',False)
    p = kwargs.pop('p',1.0)
    if p==1.0:
        return np.min(a,**kwargs),np.max(a,**kwargs)
    assert(p>0.0 and p<=1.0)
    axis = kwargs.pop('axis',None)
    aperc = lambda q: np.percentile(a,axis=axis,q=q,interpolation='nearest')
    return aperc((1-p)*100),aperc(p*100)

def cohrgba2intensity(im):
    # aka get_pixel_intensity(im) via Giangi 
    tot_px = 0
    ret = np.zeros(im.shape[0:2],dtype=np.float32)
    #this selects the interval [.4,.8)
    sel = np.abs(im[:,:,0]-1) <= 0.0 # im[:,:,0]==1.0 
    #imsel = im[np.where(sel)]
    ret[sel] = .4*(im[sel,1]+1.0)
    #print(imsel.min(),imsel.max(),ret[sel].min(),ret[sel].max())
    #this selects the interval [.8,1)
    #  NOTE (BDB, 01/05/17): actually selects [0.8,1.2) 
    sel = np.abs(im[:,:,1]-1) <= 0.0 #im[:,:,1]==1.0 #
    ret[sel] = .4*im[sel,2] + .8
    #print(imsel.min(),imsel.max(),ret[sel].min(),ret[sel].max())
    #this selects the interval [0,.4)
    sel = np.abs(im[:,:,2] - 1) <= 0.0 # im[:,:,2]==1.0 #
    ret[sel] = .4*im[sel,0]
    ret[ret>1.0] = 1.0
    #print(imsel.min(),imsel.max(),ret[sel].min(),ret[sel].max())
    return ret

if __name__ == '__main__':
    metadir = '/Users/bbue/Research/ARIA/ariamh/ariaml/meta'
    metadf = ingestmeta(metadir)

    storef = pathjoin(metadir,'metadf.h5')
    df2hdf(storef,metadf)
    
