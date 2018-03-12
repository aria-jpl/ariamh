#!/usr/bin/env python3
try:
    from ariaml.ExpFeaturesExtractor import ExpFeaturesExtractor as FE
except:
    print('Cannot import ExpFeaturesExtractor')
    pass
import json
import sys
import os
import traceback
from utils.UrlUtils import UrlUtils
from utils.contextUtils import toContext
from utils.queryBuilder import buildQuery, postQuery
from multiprocessing import pool
import numpy as np
def hasToken(tags,token):
    ret = []
    for tag in tags:
        if tag.count(token):
            ret.append(tag)
    return ret

#return the integer label associated with the str labels.
#if more than one labels return the one with most votes or None if draw 
def getIntLabel(labels):
    vals = []
    ret = None
    for i in labels:
        try:
            vals.append(int(i.split('_')[-1].split('-')[0]))
        except Exception as e:
            print(e)
    if len(vals) == 1:
        ret = vals[0]
    elif len(vals) > 1:
        vals = np.array(vals)
        low = np.min(vals) - 0.5
        high = np.max(vals) + 0.5 
        freq = np.histogram(vals,high-low,(low,high))[0]
        mval = np.max(freq)
        all = np.nonzero(freq == mval)[0]
        if len(all) == 1:
            ret = freq[all[0]]
    return int(ret) if ret is not None else ret
                     
        
def extractFeatures(inputs,label):
    process = 'extractFeatures'
    cwd = None
    try:
        url = inputs['url']
        #otherwise prdbase gets messed up
        if(url.endswith('/')):
            url = url[:-1]   
        urlsplit = url.split('/')
        prdbase = (urlsplit[-2] + '_' + urlsplit[-1]).replace('__','_')
        product = 'images_' + prdbase
        try:
            os.mkdir(product)
        except Exception:
            pass
            return
        fe = FE(url,product,.4)
        res = fe.extractFeatures()
        
        cwd = os.getcwd()
        os.chdir(product)
        json.dump({'url':url,'label':label},open(product + '.met.json','w'))
        for k,v in res['outputs'].items():
            for k1,v1 in v.items():
                v1.tofile(k1 + '_choTh_' + str(k) + '.img') 
        os.chdir(cwd)

    except Exception as e:
        if cwd:
            os.chdir(cwd)
        exitv = 10
        message = 'Failed with exception %s: %s' % (str(e), traceback.format_exc())
        toContext(process,exitv,message)

def getUrls(sensor,token):
    meta = {'dataset':'interferogram','sensor':sensor,'tags':[token]}
    ret,status = postQuery(buildQuery(meta,[]))
    inps = []
    if len(ret):
        cnt = 0
        for i in ret:
            label = getIntLabel(hasToken(i['user_tags'],token))
            if 'user_tags' in i and label is not None:
                inps.append([i['url'],label])
    return inps

def getData(sensor,token):
    meta = {'dataset':'interferogram','sensor':sensor,'tags':[token]}
    ret,status = postQuery(buildQuery(meta,[]))
    if len(ret):
        cnt = 0
        cnt1 = 0
        inps = []
        for i in ret:
            label = getIntLabel(hasToken(i['user_tags'],token))
            if 'user_tags' in i and label is not None:
                print('processing', i['url'],cnt,len(ret))
                extractFeatures(i,label)
                cnt +=  1
    print(cnt)

    '''
    pool_ = pool.Pool(processes=os.cpu_count()*2)
  
    results = []
    for inp in inps:
        results.append(pool_.apply_async(extractFeatures, (inp[0],inp[1])))
    for i in results:
        i.get()
    '''
                
                            
        
def main(infile):
    inputs = json.load(open(infile))
    getData(inputs['sensor'],inputs['token'])
    #extractFeatures(sys.argv[1])
    #url = getUrls(inputs['sensor'],inputs['token'])
if __name__ == '__main__':
    try: status = main(sys.argv[1])
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc()) 
        raise
    sys.exit(status)
