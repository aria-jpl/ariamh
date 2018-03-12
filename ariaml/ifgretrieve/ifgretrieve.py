from __future__ import absolute_import, print_function, division

import cPickle as pickle
import numpy as np
from os.path import dirname
import sys, glob
from warnings import warn

from os.path import abspath,join as pathjoin, exists as pathexists, split as pathsplit

from ariaml_util import *

from skimage.io import imread as imread
from skimage.transform import resize as imresize

ifgretrieve_home = pathsplit(__file__)[0]
ariamh_path=os.getenv('ARIAMH_HOME')
if not ariamh_path:
    ariamh_path = abspath(pathjoin(ifgretrieve_home,'../../ariamh'))
    os.environ['ARIAMH_HOME'] = ariamh_path
print('ARIAMH_PATH:',ariamh_path)
sys.path.append(pathjoin(ariamh_path,'..'))

unw20png = 'filt_topophase.unw.geo_20rad.browse.png'
unwpng = 'filt_topophase.unw.geo.browse.png'
cohpng = 'topophase_ph_only.cor.geo.browse.png'

dataset = 'interferogram'
system_version = 'v1.0.1'
platform = 'Sentinel-1A'

if platform=='Sentinel-1A':
    sensor = 'SAR-C Sentinel1'
elif platform=='CSK':
    sensor = 'csk'

ifg_dir = pathjoin(ifgretrieve_home,'..','ifg_image_cache')
query_dir = pathjoin(ifgretrieve_home,'..','ifg_query_cache')

def mdy(fmt='%m%d%y'):
    import datetime as dtime
    date = dtime.datetime.now()
    return date.strftime(fmt)

def runcmd(cmd):
    from subprocess import Popen, PIPE
    cmdstr = ' '.join(cmd) if isinstance(cmd,list) else cmd
    cmdout = PIPE
    for rstr in ['>&','>']:
        if rstr in cmdstr:
            cmdstr,cmdout = map(lambda s:s.strip(),cmdstr.split(rstr))
            cmdout = open(cmdout,'w')
            
    p = Popen(cmdstr.split(), stdout=cmdout, stderr=cmdout)
    out, err = p.communicate()
    retcode = p.returncode

    if cmdout != PIPE:
        cmdout.close()
    
    return out,err,retcode

def query_tags(tags,sensor=sensor,dataset=dataset,
               system_version=system_version,region=None,
               exclude=False,queryopts=[],verbose=False):
    from ariamh.utils.queryBuilder import postQuery, buildQuery
    if not pathexists(query_dir):
        os.makedirs(query_dir)
        
    queryf = pathjoin(query_dir,'tagquery_%s.pkl'%mdy())

    query = buildQuery({},queryopts)
    filt_terms = [
        {'dataset': dataset},
        {'metadata.sensor.untouched': sensor}
    ]

    query['filter'] = {'and': [{'term':t} for t in filt_terms]}
    query_string = ' OR '.join(map(lambda s:'(user_tag:*%s*)'%s,tags))
    if exclude:
        query_string = 'NOT (%s)'%query_string
    query_dict = {'query_string':{'query':query_string,'default_operator':'OR'}}
    query['query'] = {'bool':{'must':[query_dict]}}
    query.pop('sort')    
    
    if pathexists(queryf):
        print('loading query urls from',queryf)
        with open(queryf) as fid:
            qsaved = pickle.load(fid)
        return qsaved['urls']
    else:
        print(queryf,'unavailable, executing new query',query)
    
    qlist,qstatus = postQuery(query,system_version)
    if qstatus==False or len(qlist)==0:
        print('query failed, returned 0 products')
        return {}
    if verbose:
        print('query returned %d products'%len(qlist))
    querydict = {product.pop('url'):product for product in qlist}
    with open(queryf,'w') as fid:
        print('saving query urls to',queryf)
        pickle.dump({'query':query,'urls':querydict.keys()},fid)
    return querydict.keys()

def retrieve_ifg(ifg_urls,outdir,system_version=system_version,verbose=False):
    #curlauthf = expanduser('~/.netrc')
    #curlcmd = '/usr/bin/curl -k -f --netrc-file %s -o %s/%s %s/%s'
    curlcmd = '/usr/bin/curl -k -f --netrc -o %s/%s %s/%s'
    # traverse list of ifg urls, returning first ifg successfully retrieved
    for ifg_url in ifg_urls:
        ifg_retrieved=coh_retrieved=False
        if platform == 'CSK':
            ifg = url2pid(ifg_url).replace('interferogram_','interferogram__')
        else:
            ifg = pathsplit(ifg_url)[-1]
            
        if verbose:
            print('Retrieving %s'%ifg)
        
        ifg_base = pathjoin(outdir,ifg)
        if not pathexists(ifg_base):
            os.makedirs(ifg_base)

        ifgmet = ifg.replace('-%s'%system_version,'')+'.met.json'
        unwf = pathjoin(ifg_base,unwpng)
        unw20f = pathjoin(ifg_base,unw20png)
        cohf = pathjoin(ifg_base,cohpng)
        metf = pathjoin(ifg_base,ifgmet)
        if all([pathexists(f) for f in [unwf,unw20f,cohf,metf]]):
            if verbose:
                print('Successfully retrieved ifg url=%s'%ifg_url)
            ifg_retrieved=coh_retrieved=True
            break

        if platform!='CSK':
            cohcmd = curlcmd%(ifg_base,cohpng,ifg_url,cohpng)
        else:
            # scp Giangi's phase-only coherence mask for this ifg
            cohcmd = scpcmd%(scpauthf,ifg,cohpng,ifg_base)

        if verbose:
            print(cohcmd)
            print(curlcmd%(ifg_base,ifgmet,ifg_url,ifgmet))
            print(curlcmd%(ifg_base,unwpng,ifg_url,unwpng))
            print(curlcmd%(ifg_base,unw20png,ifg_url,unw20png))
            
        # curl the unw and unwrad20 images from the aria data store
        try:            
            _,err_met,retcode_met = runcmd(curlcmd%(ifg_base,ifgmet,ifg_url,ifgmet))
            _,err_unw,retcode_unw = runcmd(curlcmd%(ifg_base,unwpng,ifg_url,unwpng))
            if retcode_unw != 0 or retcode_met != 0:
                warn('An error occurred retrieving %s: %s'%(unwpng,err_unw))
            if retcode_unw==0 and retcode_met==0:
                ifg_retrieved=True
        except Exception as e:
            warn('An exception occurred retrieving %s: %s'%(unwpng,e))
            ifg_retrieved=False
            
        try:
            _,err_unw20,retcode_unw20 = runcmd(curlcmd%(ifg_base,unw20png,ifg_url,unw20png))            
            if retcode_unw20 != 0:
                warn('Unable to retrieve %s: %s'%(unw20png,err_unw20))
        except Exception as e:
            warn('An exception occurred retrieving %s: %s'%(unw20png,e))            
        
        try:
            _,err_coh,retcode_coh = runcmd(cohcmd)
            if retcode_coh==0:
                coh_retrieved=True
            else:
                warn('Unable to retrieve %s: %s'%(cohpng,err_coh))                
        except Exception as e:
            warn('An exception occurred retrieving %s: %s'%(cohpng,e))
            coh_retrieved=False

        if ifg_retrieved and coh_retrieved:
            if verbose:
                print('Successfully retrieved ifg url=%s'%ifg_url)
            break

    if not (ifg_retrieved and coh_retrieved):
        return None,None

    return ifg,ifg_url

def load_ifg_rgba(ifg,scalef,doplot=False,flip=False,verbose=False):
    if verbose:
        print("Loading",ifg)
    imread_plugin=None #'pil'
    unwrgba = imread(pathjoin(ifg,unwpng),plugin=imread_plugin)
    cohrgba = imread(pathjoin(ifg,cohpng),plugin=imread_plugin)

    try:
        unw20rgba = imread(pathjoin(ifg,unw20png),plugin=imread_plugin)
    except:
        warn('Could not read '+unw20png+'. Using '+unwpng+' instead')
        unw20rgba = unwrgba.copy()

    if flip:
        unwrgba   = np.flipud(unwrgba)
        unw20rgba = np.flipud(unw20rgba)
        cohrgba   = np.flipud(cohrgba)

    if verbose:
        print('unwrgba shape (orig):',unwrgba.shape)
        print('unw20rgba shape (orig):',unw20rgba.shape)
        print('cohrgba shape (orig):',cohrgba.shape)
    r,c = map(lambda x: int(x*scalef),cohrgba.shape[:2])
    
    coh2unw20dims,coh2unwdims = [],[]
    for i in [0,1]:
        if cohrgba.shape[i]!=unw20rgba.shape[i]:
            coh2unw20dims.append(i)
        if cohrgba.shape[i]!=unwrgba.shape[i]:
            coh2unwdims.append(i)

    if verbose:
        if len(coh2unw20dims)!=0:
            msg='coh and unw20 image dimensions %s do not match'%coh2unw20dims
            warn(msg)
        if len(coh2unwdims)!=0:
            msg='unw and coh image dimensions %s do not match'%coh2unwdims
            warn(msg)        
        print('resizing unw, unw20, coh images to %s'%str([r,c]))

    #coh = get_pixel_intensity(np.float32(cohrgba)/255.0)
    rsorder   = 0 # nn interpolation
    cohrgba   = imresize(cohrgba[2:-2,2:-2,:],[r,c,4],order=rsorder)
    unwrgba   = imresize(unwrgba,[r,c,4],order=rsorder)
    unw20rgba = imresize(unw20rgba,[r,c,4],order=rsorder)
    if verbose:
        print('shape (resized):',cohrgba.shape)

    # trim zeroed rows/cols
    cohmask = (cohrgba[:,:,0]==0) & (unwrgba.sum(axis=2)==0)
    keeprows = ~cohmask.all(axis=1)
    keepcols = ~cohmask.all(axis=0)

    cohrgba = np.uint8(255*(cohrgba[keeprows,:,:][:,keepcols,:])).copy()
    unwrgba = np.uint8(255*(unwrgba[keeprows,:,:][:,keepcols,:])).copy()
    unw20rgba = np.uint8(255*(unw20rgba[keeprows,:,:][:,keepcols,:])).copy()

    if verbose:
        print('shape (crop):',cohrgba.shape)
    #unwv = unwrgba.view(dtype=np.uint32)[:,:,0].copy() #.reshape([r,c]).copy()
    #unw20v = unw20rgba.view(dtype=np.uint32)[:,:,0].copy() #.reshape([r,c]).copy()
    coh = np.uint8(255*cohrgba2intensity(cohrgba/255.0))

    if doplot:
        plplotw = coh.shape[1]
        ploth = int(coh.shape[0]*(plplotw/float(coh.shape[1])))

        import pylab as pl
        fs = (plplotw/40.0,ploth/40.0)
        fig,ax = pl.subplots(2,2,sharex=True,sharey=True,figsize=fs)
        ax[0,0].imshow(unwrgba)
        ax[0,1].imshow(cohmask)
        ax[1,0].imshow(coh)
        ax[1,1].imshow(cohrgba)
        pl.show()
        
    return unwrgba, unw20rgba, coh

if __name__ == '__main__':
    verbose=True
    ifg_entries = query_tags(tags=['UWE','TNS','TNT','TNR'],
                             exclude=True,verbose=verbose)
    #ifg_entries = loadtxt(ifg_urlf,dtype=str)
    #ifg_entries = list(np.random.permutation(ifg_entries))

    log_dir = './logs'
    logf = pathjoin(log_dir,'ifgretrieve_log%s.txt'%mdy())
    with open(logf,'w') as fid:    
        for i,ifg_url in enumerate(ifg_entries):
            ifg_id = url2pid(ifg_url) if platform=='CSK' else pathsplit(ifg_url)[-1]
            print('Retrieving IFG',ifg_id,'(%d of %d)'%(i+1,len(ifg_entries)))
            ifg,entry = retrieve_ifg([ifg_url],ifg_dir,verbose=verbose)
            if ifg is None:
                print('IFG',ifg_url,'not retrieved',file=fid)
            else:                
                print('IFG',ifg_url,'retrieved successfully',file=fid)
                
