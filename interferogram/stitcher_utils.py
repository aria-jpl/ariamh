#!/usr/bin/env python3
from builtins import str
from builtins import range
from utils.queryBuilder import buildQuery, postQuery
import argparse
import os
import sys
import re
from utils.UrlUtils import UrlUtils
from interferogram.ifg_stitcher import main as main_st
import json
import numpy as np
import subprocess as sp
from datetime import datetime, timedelta
import fractions
def get_data_from_url(url):
    uu = UrlUtils()
    command = 'curl -k -f -u' + uu.dav_u + ':' + uu.dav_p + ' -O ' + url
    ntrials = 4
    failed = True
    for i in range(ntrials):
        p = sp.Popen(command,shell=True)
        try:
            p.wait(60 + i*20)
            failed = False
            break
        except Exception as e:
            print(e)
            p.kill()
    return failed
def rm_incomplete_swaths(urls,nsw=3):
    i = 0
    ret = []
    while True:
        base = urls[i].split('/')[-1].split('_')[4]
        #check if there are nsw subswaths
        cnt = 1
        for j in range(i+1,len(urls)):
            if urls[j].count(base):
                cnt += 1
            else:
                break
        if cnt == nsw:
            ret.extend(urls[i:i+nsw])
            i += nsw
        else:
            i += 1
        if i >= len(urls) - 1:
            break
    return ret

#there are some ifgs with same master and different slave names but are actcually the same slave
#just check if the slaves have the same date, if so pick only one
def rm_dups(urls,durls,swaths=None):
    if swaths is None:
        swaths = [1,2,3]
    names = []
    ms = {}
    ret= [] 
    for u in urls:
        names.append(u.split('/')[-1].split('_')[4])
    names, indx = np.unique(np.array(names),True)
    urls = np.array(urls)[indx]
    for i,name in enumerate(names):
        key = name.split('-')[0]
        sl = name.split('-')[1]
        if key in ms:
            ms[key].append([sl,urls[i]])
        else:
            ms[key] = [[sl,urls[i]]]
    #for those with more than one slave check if the slaves are in the same day
    nurls = []
    for k,v in list(ms.items()):
        if len(v) > 1:
            sl = []
            ur = []
            for d in v:
                sl.append(d[0].split('T')[0])
                ur.append(d[1])
            ss,ii = np.unique(np.array(sl),True)
            ur = np.array(ur)
            nurls.extend(ur[ii])
        else:
            nurls.append(v[0][1])
    for u in nurls:
        ms,sl = get_dates(u,True)
        keys = []
        #only add those that have completed swaths
        for i in swaths:
            if ms+sl+str(i) in durls:
                keys.append(ms+sl+str(i))
                
        if len(keys) == len(swaths):
            for k in keys:
                ret.append(durls[k])

    return sorted(ret)

def get_dates(url,sec=False):
    dates = url.split('/')[-1].split('_')[4].split('-')
    if not sec:
        #remove the senconds
        ret = []
        for d in dates:
            ret.append(d.split('T')[0])
    else:
        ret = dates
    return ret

def get_urls_sets(urls,coords,nscenes):
    i = 0
    ret = []
    while True:
        mdate,sdate = get_dates(urls[i])
        keep = []
        while True:
            if urls[i].count('_' + mdate) and urls[i].count('-' + sdate):
                keep.append(urls[i])
            else:
                break
            i += 1
            if i >= len(urls):
                break
        #simple version just check the right number of scenes
        if len(keep) == nscenes:
            ret.append(keep)
        if i >= len(urls):
            break
    return ret
          
def get_urls_sets_dev(urls,coord,swaths,aoi):
    #first organize data by dates
    dates2url = {}
    for u in urls:
        ms,sl = get_dates(u)
        if ms + '-' + sl in dates2url:
            dates2url[ms + '-' + sl].append(u)
        else:
            dates2url[ms + '-' + sl] = [u]
    #for each date make sure its complete
    dates_complete = {}
    for k,v in list(dates2url.items()):
        #create a map with only images from each swath
        swath2url = {}
        dates_complete[k] = {}
        for u in v:
            sw = swaths[u]
            if sw in swath2url:
                swath2url[sw].append(u)
            else:
                swath2url[sw] = [u]
        complete = True
        #for each swath test for completeness between the aoi limits
        #use the union of all the index that cover for each subswaths. because
        #of some shift in latitude some might need less frames to cover. use
        #the maximum number of frames among the subswaths so they all have the
        #same number of frames
        sels = np.array([])
        for k1,v1 in list(swath2url.items()):
            v1 = sorted(v1)
            #get the latmin,max for each image
            limits = []
            for u in v1:
                limits.append([coord[u]['minLat'],coord[u]['maxLat']])
            limits = np.array(limits)
            #sort in ascending order
            indx = np.argsort(limits[:,0])
            #sorted limits
            slimits = limits[indx,:]
            #sanity check. should at least cover the extremes of aoi
            if slimits[0,0] > aoi[0] or slimits[-1,1] < aoi[1]:
                complete = False
                break    
            #create an array of 0.1 degree of the full span of the aoi
            covered = np.zeros(int(aoi[1]*10) - int(aoi[0]*10) + 1,dtype=np.int)
            #fill with ones all the extend of the array that contains data
            for lim in slimits:
                start = max(int(lim[0]*10) - int(aoi[0]*10),0)
                end = min(int(lim[1]*10),int(aoi[1]*10)) - int(aoi[0]*10)                
                sel = np.arange(start,end + 1)
                covered[sel] = 1
            #if there were data over all the aoi then the array is filled with ones
            if any(covered == 0):
                complete = False
                break
            #make sure to use the minimum number of images necessary to cover aoi     
            #start from the bottom and go up stopping at the first that goes over
            #the upper limit of the aoi
            maxi = 0
            for i,lim in enumerate(slimits):
                if lim[1] > aoi[1]:
                    maxi = i
                    break
            #do the reverse to find the first valuable frame
            mini = 0
            for i in np.arange(len(slimits))[::-1]:
                lim = slimits[i]
                if lim[0] < aoi[0]:
                    mini = i
                    break
            sels = np.union1d(sels,indx[mini:maxi+1]).astype(np.int)
        if not complete:
            dates_complete[k] = {}
        else:
            for k1,v1 in list(swath2url.items()):
                dates_complete[k][k1] = np.array(v1)[sels].tolist()

    return dates_complete
           
def sort_data(inps):
    urls = []
    durls = {}
    coords = {}
    for fid in range(inps['frameIDs'][0],inps['frameIDs'][1]+1):
        for plat in inps['platforms']:
            for sw in inps['swaths']:
                meta = {'frameID':str(fid),'trackNumber':str(inps['track']),'direction.raw':inps['direction'],
                         'dataset_type': 'interferogram',
                         'platform.raw': plat,"swath":sw}
                if 'tags' in inps:
                    meta['tags'] = inps['tags']
                query = buildQuery(meta,[])
                #sv in form 'v1.1.1'
                ret,status = postQuery(query,sv=inps['sys_ver'],conf=inps['conf'])
                for r in ret:
                    urls.append(r['url'])
                    ms,sl = get_dates(urls[-1],True)
                    durls[ms + sl + str(sw)] = r['url']
                    coords[r['url']] = r['imageCorners']
    ourls = np.unique(np.array(urls))
    urls = rm_incomplete_swaths(ourls,len(inps['swaths'])) 
    urls = rm_dups(urls,durls,inps['swaths'])  
    urls = get_urls_sets(urls,coords,  inps['nscenes'])    
    return urls

def sort_data_dev(inps):
    urls = []
    durls = {}
    coords = {}
    swaths = {}
    for plat in inps['platforms']:
        for sw in inps['swaths']:
            meta = {'trackNumber':str(inps['track']),'direction.raw':inps['direction'],
                     'dataset_type': 'interferogram','latitudeIndexMin':str(inps['latitudeIndexMin']),
                     'latitudeIndexMax':str(inps['latitudeIndexMax']),'platform.raw': plat,'swath':sw}
            options = ['cross-boundaries']
            if 'tags' in inps:
                meta['tags'] = inps['tags']
            query = buildQuery(meta,options)
            #sv in form 'v1.1.1'
            ret,status = postQuery(query,sv=inps['sys_ver'],conf=inps['conf'])
            for r in ret:
                urls.append(r['url'])
                ms,sl = get_dates(urls[-1],True)
                durls[ms + sl + str(sw)] = r['url']
                coords[r['url']] = r['imageCorners']
                swaths[r['url']] = sw

    ourls = np.unique(np.array(urls))
    urls = rm_incomplete_swaths(ourls,len(inps['swaths'])) 
    urls = rm_dups(urls,durls,inps['swaths'])  
    dates_complete = get_urls_sets_dev(urls,coords,swaths,[inps['latitudeIndexMin']/10.,inps['latitudeIndexMax']/10.])    
    urls, dates_incomplete = url_from_dates(dates_complete)
    return urls,dates_incomplete

def sort_data_from_mets(inps):
    mets = json.load(open(inps['meta_file']))
    urls = []
    durls = {}
    coords = {}
    swaths = {}
    for r in mets:
        urls.append(r['url'])
        ms,sl = get_dates(urls[-1],True)
        sw = r['swath']
        durls[ms + sl + str(sw)] = r['url']
        coords[r['url']] = r['imageCorners']
        swaths[r['url']] = sw
    ourls = np.unique(np.array(urls))
    urls = rm_incomplete_swaths(ourls,len(inps['swaths'])) 
    urls = rm_dups(urls,durls,inps['swaths'])  
    dates_complete = get_urls_sets_dev(urls,coords,swaths,[inps['latitudeIndexMin']/10.,inps['latitudeIndexMax']/10.])    
    urls, dates_incomplete = url_from_dates(dates_complete)
    return urls,dates_incomplete
    
def get_mets(inps):
    mets = []
    for plat in inps['platforms']:
        for sw in inps['swaths']:
            meta = {'trackNumber':str(inps['track']),'direction.raw':inps['direction'],
                     'dataset_type': 'interferogram','latitudeIndexMin':str(inps['latitudeIndexMin']),
                     'latitudeIndexMax':str(inps['latitudeIndexMax']),'platform.raw': plat,'swath':sw}
            options = ['cross-boundaries']
            if 'tags' in inps:
                meta['tags'] = inps['tags']
            query = buildQuery(meta,options)
            #sv in form 'v1.1.1'
            ret,status = postQuery(query,sv=inps['sys_ver'],conf=inps['conf'])
            for r in ret:
                mets.append(r)
    return mets

def url_from_dates(dates_complete):
    '''
    From a set of dates,url pairs extract a list of sorted urls by dates and a list
    of dates that are incomplete
    inputs:
        dates_complete: dict of dates,url pairs
    outputs: 
        urls, dates_incomplete: list of sorted urls that cover the aoi and lsit of incomplete dates
    '''
    dates_sorted = sorted(list(dates_complete.keys()))
    urls = []
    dates_incomplete = []
    for k in dates_sorted:
        v = dates_complete[k]
        if len(v) == 0:
            dates_incomplete.append(k)
        else:
            urls.append(v)
    return urls, dates_incomplete

def get_ts_dates(urls):
    dates = []
    for u in urls:
        dates.append(get_dates(u[0]))
    dates = np.array(dates).astype(np.int)       
    dts,indx = np.unique(dates[:,1],True)
    i = 0
    
    res = [dates[i]]
    aux = []
    guard = 0
    while i < len(dates) - 1:
        sel = np.nonzero(dates[i + 1:,1] == dates[i,0])[0]
        print(i,dates[i,:],i + 1 + sel)
        #Tracer()()
        if len(sel) == 0:
            cont = False
            guard1 = 0
            while True:
                i += 1
                if i >= len(dates):
                    break
                if guard1 == 150:
                    break
                if dates[i,1] >= dates[i-1,1] and dates[i,0] > dates[i-1,0]:
                    res.append(dates[i,:])
                    aux.append([i,len(np.nonzero(dates[i:,1] == dates[i,1])[0])])
                    cont = True
                    break
                guard1 += 1
            if cont:
                i += 1
                continue
            else:
                break
        res.append(dates[i + 1 + sel[0],:])
        aux.append([i,len(np.nonzero(dates[i:,1] == dates[i,1])[0])])
        i += sel[0] + 1
        guard += 1
        if guard == 150:
            break
    return res

def date2num(date):
    return datetime.strptime(date,'%Y%m%d').toordinal()

#find the smallest step that will sample all the dates at least once
def get_smallest_step(repeats):
    best = np.max(repeats)
    for i in range(len(repeats)-1):
        for j in range(i,len(repeats)):
            gdc = fractions.gcd(repeats[i],repeats[j])
            if gdc < best:
                best = gdc
    return best
            
def get_ts_urls(urls,min_repeat=12,max_repeat=72,only_best=True):
    '''
    Given a set if url provide a list of the minimun number of urls that cover temporally 
    the time sapn givend by the minimun and maximun date in the urls
    inputs:
        urls: list of dicts of the from {1:[url1,url2],2:[url1,url2]} where the
              key is the swath number and the urls are the urls for that date and swath
        min_repeat: use ifg with repeat of at least min_repeat days
        max_repeat: use ifg with repeat of at the most min_repeat days
    outputs:
        output_urls: list with elements of urls that cover the temporal span
    '''
    dates = []
    urls = np.array(urls)
    for u in urls:
        #get one of the key and use that to get the value. from any of
        #the urls we can get the dates
        v = u[list(u.keys())[0]]
        dates.append(get_dates(v[0]))
    ndates = []
    for d in dates:
        ndates.append([date2num(d[0]),date2num(d[1])])
    ndates = np.array(ndates)
    min_date = dates[0][0]
    if ndates[0,0] > ndates[0,1]:
        ndates = np.fliplr(ndates)
        min_date = dates[0][1]
    #make it start from zero
    ndates = ndates - np.min(ndates[0,:])
    #order by first column, then second. lextsort use last element as primary key ergo the flip
    indx = np.lexsort((ndates[:,1],ndates[:,0]))
    ndates = ndates[indx,:]
    
    #make the left column the earliest
    
    repeats = np.diff(ndates,1)
    repeats = np.reshape(repeats,(repeats.shape[0],))
    #find the unique repeats
    urepeats = np.unique(repeats[np.logical_and(repeats >= min_repeat,repeats <= max_repeat)])
    sur = urepeats.size
    occ = np.zeros((sur,np.max(ndates))).astype(np.int)
    occ2repeat = np.zeros((sur,np.max(ndates))).astype(np.int)
    #for each repeat check the dates that it covers and give it a 
    #unique identifiers (sur - 1)**i
    for i in range(sur):
        sel = np.nonzero(urepeats[i] == repeats)[0]
        for j in sel:
            occ[i,ndates[j,0]:ndates[j,1]] = (sur - 1)**i
            occ2repeat[i,ndates[j,0]:ndates[j,1]] = j 
    #for now only proceed if it's all covered, eventually use largest chuck
    non_covered = np.nonzero(np.max(np.cumsum(occ,0),0).astype(np.int) == 0)[0]
    if len(non_covered) > 0:
        ret = []
        for i in non_covered:
            fmt = '%Y%m%d'
            ret.append(datetime.strftime(datetime.strptime(min_date,fmt) + timedelta(days=int(i)),fmt))
        return [],ret
    if only_best:
        #get the smallest step to sample the date coverage
        step = get_smallest_step(urepeats)
        #sample the date occupancy and check where is the smallest value
        #that covers it. smallest value means shortest repeat.
        #start inside the interval
        sel = []
        for i in range(0,occ.shape[1],step):
            #note that there is always a result since the min value
            #cannot be xero from the check before
            sel.append(np.nonzero(np.cumsum(occ[:,i]) > 0)[0][0])
        sel = np.array(sel)
        cov = []
        for j,i in enumerate(range(0,occ.shape[1],step)):
            ni = occ2repeat[sel[j],i]
            cov.append(ni)
        seldates = np.unique(np.array(cov))
    else:
        seldates = np.arange(indx.size)
            
    return urls[indx[seldates]]
            
def donwload(unw_name,frames,dirname,products):
    try:
        os.mkdir(dirname)
    except:
        pass
    cwd = os.getcwd()
    os.chdir(dirname)
    fnames = []
    failed = False
    for i,urls in list(frames.items()):
        fname = []
        for j,v in enumerate(urls):
            rundir = 'run_' + str(j+1) + '_' + str(i)
            fname.append(os.path.join(rundir,unw_name))
            try:
                os.mkdir(rundir)
            except:
                pass
            ccwd = os.getcwd()
            os.chdir(rundir)
            for pr in products:
                failed = get_data_from_url(v + '/merged/' + pr)
                failed1  = get_data_from_url(v + '/merged/' + pr + '.xml')
                if failed is True or failed1 is True:
                    print('Stitching Failed')
                    failed = True
                    break
            os.chdir(ccwd)
            if failed:
                break
        if failed:
            break
        fnames.append(fname)
    fnames = np.array(fnames).T.tolist()
    if not failed:
        ret = fnames
    else:
        ret = []
    os.chdir(cwd)
    return ret
#input is a json with the direction (along,across), the output filename (filt_topophase.geo) and the 
#the list of the input files.
#assumes that we are already in the working dir all all inputs are localized
def stitch(inp_json):
    #jdict = {'direction':'along','outname':'filt_topophase.unw.geo'}
    main_st(inp_json)

def parse(inps):
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-a', '--action', type = str, default = 'stitch', dest = 'action', help = 'Possible actions: stitch or download or validate,(default: %(default)s). ')
    parser.add_argument('-i', '--input', type = str, required=True, dest = 'input', help = 'Input file name ')

    return parser.parse_args(inps)

def main(args):
    iargs = parse(args)
    inps = json.load(open(iargs.input))
    if iargs.action == 'validate_ifg_json':
        urls,dates_incomplete = sort_data_dev(inps)
        out_json = {'urls_list':urls,'dates_incomplete':dates_incomplete}
        json.dump(out_json,open(inps['output_file'],'w'), indent=2, sort_keys=True)
    elif iargs.action == 'validate_ifg_met':
        urls,dates_incomplete = sort_data_from_mets(inps)
        out_json = {'urls_list':urls,'dates_incomplete':dates_incomplete}
        json.dump(out_json,open(inps['output_file'],'w'), indent=2, sort_keys=True)
    elif iargs.action == 'validate_ts_json':
        urls,dates_incomplete = sort_data_dev(inps)
        res = get_ts_urls(urls,inps['min_repeat'],inps['max_repeat'])
        if len(res[0]) == 0:#no full coverage, second item is a list of
            out_json = {'gaps':res[1],'dates_incomplete':dates_incomplete}
        else:
            out_json = {'urls_list':res.tolist(),'dates_incomplete':dates_incomplete}
        json.dump(out_json,open(inps['output_file'],'w'), indent=2, sort_keys=True)
    elif iargs.action == 'validate_ts_met':
        urls,dates_incomplete = sort_data_from_mets(inps)
        res = get_ts_urls(urls,inps['min_repeat'],inps['max_repeat'])
        if len(res[0]) == 0:#no full coverage, second item is a list of
            out_json = {'gaps':res[1],'dates_incomplete':dates_incomplete}
        else:
            out_json = {'urls_list':res.tolist(),'dates_incomplete':dates_incomplete}
        json.dump(out_json,open(inps['output_file'],'w'), indent=2, sort_keys=True)
    elif iargs.action == 'download':
        frames  = inps['urls']
        dirname = inps['dirname']
        unw_name = inps['outname']
        products = inps['products'] 
        ifg_names = inps['ifg_names']
        fnames = donwload(unw_name,frames,dirname,products)
        json.dump(fnames,open(ifg_names,'w'), indent=2, sort_keys=True)
    elif iargs.action == 'stitch':
        stitch(iargs.input)
    else:
        print('Unrecognized option',iargs.action)
        raise ValueError
        
        
    
    
if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
