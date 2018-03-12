#!/usr/bin/env python3
import os
from matplotlib import pyplot as plt
import numpy as np
import sys
import json
from matplotlib import pyplot as plt

def findCropLimits(dire,filename,size):
    ls = os.listdir(dire)
    #bbox is the (x,y) of the lower and upper corner to crop  
    bbox = np.array([size[0:2],[0,0]])
    for f in ls:
        name = os.path.join(dire,f,filename)
        if not os.path.exists(name):
            continue
        im = np.reshape(np.fromfile(name),size)
        #looking for zero columns
        indx = np.unique(np.nonzero(np.sum(im,0))[0])
        if len(indx):
            lowerx = max(indx[0]-1,0)
            upperx = min(indx[-1] + 1, im.shape[1] - 1)
            if lowerx < bbox[0][0]:
                bbox[0][0] = lowerx
            if upperx > bbox[0][1]:
                bbox[0][1] = upperx  
        #looking for zero rows
        indx = np.unique(np.nonzero(np.sum(im,1))[0])
        if len(indx):
            lowery = max(indx[0]-1,0)
            uppery = min(indx[-1] + 1, im.shape[0] - 1)
            if lowery < bbox[1][0]:
                bbox[1][0] = lowery
            if uppery > bbox[1][1]:
                bbox[1][1] = uppery
        #check if the limits include the full image. if so break
        if np.all(bbox - np.array([[0,0],size[0:2]]) == 0):
            break  
    return bbox

def getLabel(dire):
    ls = os.listdir(dire)
    label = None
    for f in ls:
        if f.endswith('met.json'):
            try:
                inps = json.load(open(os.path.join(dire,f)))
                label = inps['label']
                break
            except:
                break
    return label 
def loadAndCrop(dire,inputs,bbox = None):
    ls = os.listdir(dire)
    inps = json.load(open(inputs))
    ret = []
    labels = []
    names = []
    imRef = None
    for f in ls:
        if f.count('images_interferogram'):
            label = getLabel(os.path.join(dire,f))
        else:
            label = None
        if label is None:
            continue
        channels = []
        skip = False
        firstTime = True
        for k in sorted(inps.keys()):
            name = os.path.join(dire,f,k)
            if not os.path.exists(name):
                skip = True
                break
            im = np.reshape(np.fromfile(name),inps[k])
            if bbox is not None:
                im = im[bbox[1][0]:bbox[1][1],bbox[0][0]:bbox[0][1],:]
            for i in range(im.shape[2]):
                channels.append(im[:,:,i])
            if firstTime:
                names.append(f)
                firstTime = False
                #plt.imshow(im[:,:,0])
                #plt.show()
        if skip:
            continue
        ret.append(channels)
        labels.append(label)
    return ret,labels,sorted(inps.keys()),names
#argv[1] = data directory
#argv[2] = input json
def main():
    import time
    bbox = findCropLimits(sys.argv[1],'coherenceDist_choTh_4.img',[150,200,2])
    #if at least 5 px of difference is not worth  or all more then 2 px
    if (not np.any(np.abs(bbox - np.array([[0,200],[0,150]])) >= 5)) or (not np.all(np.abs(bbox - np.array([[0,200],[0,150]])) >= 2)):
        bbox = None
    ret,labels,lst,names = loadAndCrop(sys.argv[1],sys.argv[2],bbox)
    ret = np.array(ret,np.float32)
    labels = np.array(labels,np.int32)
    ret = np.transpose(ret,[0,2,3,1])
    ret.tofile('conv_features_' + '_'.join([str(i) for i in ret.shape]) + '.fts')
    labels.tofile('conv_labels_' + '_'.join([str(i) for i in labels.shape]) + '.lab')
    json.dump(lst,open('feature_order.ord','w'))
    with open('features_name.txt','w') as fp:
        for i in names:
            fp.write(i + '\n')
    '''
    import matplotlib.animation as animation
    fig1 = plt.figure()
    ims = []
    for i in range(10):
        inx = np.random.randint(len(ret[i]) - 2)
        ims.append((plt.imshow(ret[inx][0]),))
    im_ani = animation.ArtistAnimation(fig1, ims, interval=50, repeat_delay=300,
    blit=True)
    plt.show()
    '''    
if __name__ == '__main__':
    sys.exit(main())