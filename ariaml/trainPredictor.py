#!/usr/bin/env python3
import sys
import os
import json
import pickle
import traceback
import numpy as np
import time
import datetime as dtime

from progressbar import ProgressBar, ETA, Bar, Percentage

from sklearn.base import clone
from sklearn.preprocessing import MinMaxScaler
from sklearn.cross_validation import KFold, StratifiedKFold
from sklearn.grid_search import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_score, recall_score

from utils.UrlUtils import UrlUtils
#from utils.contextUtils import toContext
def toContext(process,exitv,message):
    print(process,exitv,message)

pathjoin = os.path.join
pathexists = os.path.exists
mdy = dtime.datetime.now().strftime('%m%d%y')

product_type  = 'interferogram'
cache_dir     = 'cached'

train_folds   =  np.inf # inf = leave-one-out, otherwise k-fold cross validation
train_state   =  42 # random seed
train_verbose =  0
train_jobs    = -1

cv_type = 'loo' if train_folds==np.inf else '%d-fold'%train_folds
cv_probs = True # record prediction probabilities in addition to labels

scorefn = {} # map from name (e.g., mse) -> f(y_true,y_pred)
scorefn['precision'] = lambda te,pr,ul: precision_score(te,pr,labels=ul)
scorefn['recall']    = lambda te,pr,ul: recall_score(te,pr,labels=ul)

errorfn = {} # map from name (e.g., diff) -> f(y_true,y_pred)
errorfn['match'] = lambda y_true,y_pred: y_true==y_pred

# GRID SEARCH PARAMS FOR PARAMETER TUNING ######################################
gridcv_folds   =   2 # number of cross-validation folds per gridcv parameter 
gridcv_jobs    =  -1  # -1 = use all cores
gridcv_verbose =   0 # verbosity level of model-tuning cross-validation output
gridcv_score   =  'roc_auc'

# SKLEARN MODEL SPECIFICATIONS #################################################
### Random Forest ##############################################################
rf_trees = 500
rf_feats = np.linspace(0.1,1.0,5)
rf_depth = [2,4,7,10,25]
rf_jobs  = 1 if gridcv_jobs == -1 else -1 # multiprocessing + RandomForest don't play nice

rf_tuned = {'max_features':rf_feats,'max_depth':rf_depth}
rf_defaults = {
    'n_estimators': rf_trees,'max_features':'sqrt','n_jobs':rf_jobs,
    'verbose':train_verbose,'random_state':train_state,
    'criterion':'gini','class_weight':'balanced_subsample'
}

### XGBoost ####################################################################
xgb_depth = [3,4,5,10,25]
xgb_subsample = np.linspace(0.1,1,5)
xgb_default = {
    'n_estimators':rf_trees,'max_delta_step':1,'learning_rate':0.1,
    'objective':'binary:logistic','max_depth':3,'subsample':0.5,
    'colsample_bytree':1,'subsample':1,'silent':(not train_verbose),
    'seed':train_state,'nthread':train_jobs
}
xgb_tuned = {'learning_rate':[0.001,0.01,0.05,0.1,0.25,0.33],
             'max_depth':xgb_depth,'subsample':xgb_subsample}


def loadjson(jsonfile):
    with open(jsonfile,'r') as fid:
        return json.load(fid)

def dumpjson(objdict,jsonfile):
    with open(jsonfile,'w') as fid:
        return json.dump(fid,objdict)    

def url2pid(url):
    """
    url2pid(url): convert url to product id
    
    Arguments:
    - url: url to convert
    
    Keyword Arguments:
    None
    
    Returns: 
    - product id for url
    """
    
    if url.endswith('/'):
        url = url[:-1]
    urlsplit = url.split('/')
    return (urlsplit[-2] + '_' + urlsplit[-1]).replace('__','_')

def url2featid(url,product_type):
    """
    url2pid(url): convert url to feature id
    
    Arguments:
    - url: url to convert
    
    Keyword Arguments:
    None
    
    Returns: 
    - feature id for url
    """    
    return url.replace(product_type,'features').replace('features__','features_'+product_type+'__')

def fdict2vec(featdict,clfinputs):
    '''
    extract feature vector from dict given classifier parameters
    specifying which features to use
    '''
    
    fvec = []
    try:
        featspec  = clfinputs['features']
        featorder = featspec['feature_order']
        featdims  = featspec['feature_dims']
        cohthr    = featspec['cohthr10']    
        featscoh  = featdict['%d'%cohthr]

        for fid,fdim in zip(featorder,featdims):
            flist = featscoh[fid]
            if not isinstance(flist,list):
                flist = [flist]
            assert(len(flist) == fdim)
            fvec.extend(flist)
    except Exception:
        pass

    return fvec

def curlProductMeta(prod_url,verbose=False,remove=True):
    """
    curlProductMeta(prod_url,verbose=False) 
    
    Arguments:
    - prod_url: product url
    
    Keyword Arguments:
    - verbose: verbose output (default=False)
    
    Returns: metadata dict from product .met.json
    
    """    
    if prod_url.endswith('/'):
        prod_url = prod_url[:-1]
        
    prod_json = url2pid(prod_url)  + '.met.json'
    try:
        uu = UrlUtils()
        silentoutput = ' ' if verbose else ' --silent '
        userstr = uu.dav_u + ':' + uu.dav_p
        command = 'curl' + silentoutput + '-k -f -u' + userstr + ' -O ' + pathjoin(prod_url,prod_json)
        os.system(command)
    except Exception:        
        return {}

    if not pathexists(prod_json):
        return {}
    
    meta = loadjson(prod_json)
    if remove:
        os.remove(prod_json)
    return meta

def getFeatures(url,clfinputs,product_type='interferogram'):
    '''
    retrieves feature vector for the given product url, provided clfinputs
    '''
    featurl = url2featid(url,product_type)
    featdict = curlProductMeta(featurl)
    fvec = fdict2vec(featdict,clfinputs)
    return fvec

def loadQuery(querymeta,queryoptions=[],queryoutfile=None,cache=False):
    '''
    builds/posts the faceted search query specified in querymeta and dumps the
    result to queryoutfile. if queryoutfile already exists, the query is loaded from
    disk rather than executed.
    '''
        
    if not cache or not pathexists(queryoutfile):
        print('executing faceted search query...')
        from utils.queryBuilder import postQuery, buildQuery
        from utils.contextUtils import toContext
        ret,status = postQuery(buildQuery(querymeta,queryoptions))
        if cache and status:
            # only dump the query if caching enabled and postQuery succeeds
            with open(queryoutfile,'wb') as fid:
                pickle.dump(ret,fid)
    elif cache:
        print('loading cached query from %s...'%queryoutfile)
        with open(queryoutfile,'rb') as fid:
            ret = pickle.load(fid)
    print('query returned %d products'%len(ret))
    return ret

def loadClassmap(cmapjson):
    """
    loadClassmap(cmapjson) - loads classmap file,
    substitutes '_', for '-' as necessary
    
    Arguments:
    - cmapjson: classmap .json file
    
    Keyword Arguments:
    None
    
    Returns: classmap with substitutions
    
    """
    
    initialmap = loadjson(cmapjson)
    classmap = initialmap.copy()
    
    # substitute '-' with '_' (for user-tagged typos)
    tags = initialmap.keys()
    for tag in tags:
        if '-' in tag:
            classmap[tag.replace('-','_')] = classmap[tag]
            
    return classmap

def loadPredictorSpec(clfjson):
    """
    loadPredictorSpec(clfjson) 
    
    Arguments:
    - clfjson: json file specifying classifier parameters
    
    Keyword Arguments:
    None
    
    Returns: dict containing classifier parameters,
    including (but not limited to):
    - classmap: classmap to map user tags to labels
    - features: dict containing information about features used to train classifier
    
    """
    
    clfspec = loadjson(clfjson)
    clfspec['classmap'] = loadClassmap(clfspec["classmap_file"])
    clfspec['features'] = loadjson(clfspec["feat_file"])
    return clfspec

def dumpPredictorSpec(inputs):
    clfspec = {}
    clfspec['clf_file'] = inputs['clf_name']+'.pkl'
    for key in ['clf_type','classmap','feat_file']:
        clfspec[key] = inputs[key]

    json.dump(clfspec,inputs['clf_name']+'.json')

def PredictorSpec(inputjson):
    
    clfspec['clf_file'] = inputs['clf_file']
    clfspec['classmap'] = inputs["classmap_file"]
    clfspec['features'] = inputs("feat_file")
    

def usertags2label(usertags,classmap):
    '''
    return dictionary of matched (tag,label) pairs in classmap for all tags 
    returns {} if none of the tags are present in classmap 
    '''
    labelmap = {}
    for tag in usertags:
        tag = tag.strip()
        for k,v in classmap.items():
            if tag.count(k):
                labelmap[tag] = v
            
    return labelmap

def queryAllTags(taglist,cache=False):
    '''
    return all urls with user tags present in taglist
    '''
    tagpkl = pathjoin(cache_dir,"usertags.pkl")
    tagquery = {'dataset_type':product_type,'tags':taglist}
    querylist  = loadQuery(tagquery,cache=cache,queryoutfile=tagpkl)
    querydict = {}
    for product in querylist:
        purl = product['url']
        querydict[purl] = product

    return querydict

def collectUrlTags(urllist,querymeta={}):        
    """
    collectUrlTags(urllist,querymeta={}) 

    collects user tags for a list of urls
    
    Arguments: 
    - urllist: list of urls
    
    Keyword Arguments:
    - querymeta:  (default={})
    
    Returns: dict keyed on product id containing
    - url: input url
    - user_tags: tags for input url
    
    """
    
    tagdict = {}
    nurl = len(urllist)
    for i,url in enumerate(urllist):
        if url in querymeta: # use the query input if possible
            meta = querymeta[url]
        else: # otherwise retrieve product metadata via curl
            meta = curlProductMeta(url)
        tagdict[url2pid(url)] = {'url':url,'user_tags':meta.get('user_tags',[])}
        
    return tagdict



def collectTrainingData(urls,clfinputs,cache=False):
    '''
    construct matrix of training samples X with labels y by intersecting the set of
    IGMs with extracted features (featquery) with the set of tagged IGMs (taggedquery)

    Keep only IGMs with tags present in classmap, and select/validate features
    according to the parameters in clfinputs.

    Returns: dict containing:
    - tags: list of user tags used to select training samples
    - X, y: training samples, labels
    - traintags: tags for each training sample
    - trainurls: url for each training sample
    - skiplist: list of urls which could not be retrieved due to errors
    - errors: list of error strings for each url in skiplist
    '''
    classmap = clfinputs['classmap']
    tags = sorted(list(classmap.keys()))

    traindatpkl = pathjoin(cache_dir,"traindat.pkl")

    if cache and pathexists(traindatpkl):
        print('loading training data from %s...'%traindatpkl)
        with open(traindatpkl,'rb') as fid:
            ret = pickle.load(fid)
            # make sure the set of tags match
            if all([ret['tags'][i] == tags[i] for i in range(len(tags))]):
                return ret

    print("querying %d tags"%len(tags))
    querymeta = queryAllTags(tags,cache=cache)
    if len(urls)==0:
        print('no URLs provided, training using all tags in classmap')
        # construct/run query to get metadata for all products with given tags
        urls = list(querymeta.keys())
    elif isinstance(urls,str):
        urls = [urls]
    
    tagdict = collectUrlTags(urls,querymeta=querymeta)
    ntagged = len(tagdict)
    
    X,y = [],[]
    traintags,trainurls = [],[]
    errors,skiplist = [],[]
    
    widgets = ['collecting features for %d products'%ntagged, Percentage(), ' ', Bar('='), ' ', ETA()]
    pbar = ProgressBar(widgets=widgets, maxval=ntagged).start()
    for i,pid in enumerate(tagdict):        
        tdict = tagdict[pid]
        turl,ttags = tdict['url'],tdict['user_tags']
        taglabel = usertags2label(ttags,classmap)
        if len(taglabel) == 0:
            continue
        fvec = getFeatures(turl,clfinputs)
        if len(fvec)==0:
            errmsg = "error collecting features for product %s (skipped)"%pid
            errors.append(errmsg)
            skiplist.append(turl)
            continue     
        pidtags,pidlabs = list(taglabel.keys()),list(taglabel.values())
        if len(pidtags) == 1:
            X.append(fvec)
            y.append(pidlabs[0])
            traintags.append(pidtags[0])
            trainurls.append(turl)
        elif len(pidtags) > 1:
            ulab = np.unique(pidlabs)
            if len(ulab) == 1:
                X.append(fvec)
                y.append(pidlabs[0])
                traintags.append(pidtags[0])
                trainurls.append(turl)
            else:
                errmsg = "conflicting tags (%s) for product %s, skipped"%(pidtags,pid)
                errors.append(errmsg)
                skiplist.append(turl)
        pbar.update(i)

    pbar.finish()
    # sort products by product url to ensure identical ordering of X,y
    sorti = np.argsort(trainurls)
    print('collected', len(sorti), 'training samples (skipped %d)'%len(skiplist))
    X,y = np.array(X)[sorti,:],np.array(y)[sorti]
    traintags,trainurls = np.array(traintags)[sorti],np.array(trainurls)[sorti]
    ret = {'tags':tags,'X':X,'y':y,'traintags':traintags,'trainurls':trainurls,
           'skiplist':skiplist,'errors':errors}

    if cache:
        with open(traindatpkl,'wb') as fid:
            pickle.dump(ret,fid)
        print('saved training data to %s'%traindatpkl)
    
    return ret

def train(X_train,y_train,clfinputs,**kwargs):
    """
    train(X_train,y_train,clfinputs,**kwargs) 

    train a classifier with parameter tuning via gridsearchcv 

    Arguments:
    - X_train: training data (N x n matrix)
    - y_train: training labels (N x 1 vector)
    - clfinputs: classifier spec
    
    Keyword Arguments:
    None
    
    Returns:
    - clf: tuned classifier
    - cv: cross validation struct used to tune classifier
    
    """
    
    uy = np.unique(y_train)
    if len(uy) != 2:
        print('error: need 2 classes for classification!')
        return None,None 
    
    model_id = clfinputs['clf_type']
    if model_id == 'rf':
        model_clf = RandomForestClassifier(**rf_defaults)
        model_tuned = [rf_tuned]
    else:
        print("invalid clf_type")
        return {}
    
    clf = clone(model_clf)
    if model_tuned is not None and len(model_tuned) != 0 and \
       len(model_tuned[0]) != 0: 
        cv = GridSearchCV(clf,model_tuned,cv=gridcv_folds,scoring=gridcv_score,
                          n_jobs=gridcv_jobs,verbose=gridcv_verbose,refit=True)
        cv.fit(X_train, y_train)
        clf = cv.best_estimator_
    else: # no parameter tuning
        clf.fit(X_train,y_train)

    return clf,cv

def crossValidatePredictor(X,y,clfinputs,logfile='cvout.log'):
    """
    crossValidatePredictor(X,y,clfinputs,logfile='cvout.log') 

    use cross validation to assess the quality of a specified classifier

    Arguments:
    - X: training data
    - y: training labels
    - clfinputs: dict of classifier inputs
    
    Keyword Arguments:
    - logfile: cross-validation outfile (default='cvout.log')
    
    Returns:
    - dict containing:
      - models: model for each cross validation fold
      - scores: scores for each fold according to each scorefn
      - preds: predictions for each training sample
      - errors: errors for each training sample according to each errorfn
      - modelcvs: cross validation structure used to train each model
    """
        
    models,modelcvs,preds,probs = [],[],[],[]
    scores = dict([(key,[]) for key in scorefn.keys()])
    errors = dict([(key,[]) for key in errorfn.keys()])

    # validate class labels
    uy = np.unique(y)
    if len(uy) != 2:
        print('error: need 2 classes for classification!')
        return {}
    
    N,ymin = len(y),uy[0]

    if cv_type == 'loo':
        cv = KFold(N,n_folds=N,random_state=train_state)
        y_pred = np.zeros(N)
        y_prob = np.zeros(N)
    else:        
        cv = StratifiedKFold(y,n_folds=train_folds,random_state=train_state)

    n_folds = len(cv)        
    model_id = clfinputs['clf_type']
    widgets = ['%s cv: '%cv_type, Percentage(), ' ', Bar('='), ' ', ETA()]
    pbar = ProgressBar(widgets=widgets, maxval=n_folds+(cv_type=='loo')).start()
    with open(logfile,'w') as logfid:
        cv_test_index = []
        scorekeys = sorted(scores.keys())
        for i,(train_index,test_index) in enumerate(cv):
            pbar.update(i)
            X_train, X_test = X[train_index], X[test_index]
            y_train, y_test = y[train_index], y[test_index]

            cv_test_index.extend(test_index) 
                            
            # xgb assumes labels \in {0,1}
            if model_id == 'xgb' and ymin == -1:                
                y_train[y_train==-1] = 0                

            # train/predict as usual
            clf,clf_cv = train(X_train,y_train,clfinputs)
            clf_pred = clf.predict(X_test)
            if model_id == 'xgb' and ymin == -1:
                clf_pred[clf_pred==0] = -1

            if cv_probs:
                clf_prob = clf.predict_proba(X_test)[:,0]
            else:
                clf_prob = np.ones(len(clf_pred))*np.nan
                
            # loo predicts one label per 'fold'
            if cv_type == 'loo':

                y_pred[test_index] = clf_pred
                y_prob[test_index] = clf_prob
                # compute scores for the points we've classified thus far
                y_test_cur = np.atleast_1d(y[cv_test_index])
                y_pred_cur = np.atleast_1d(y_pred[cv_test_index])
                
                for score,score_fn in scorefn.items():
                    scorei = score_fn(y_test_cur,y_pred_cur,uy)
                    scores[score] = [scorei]                
            else:
                # collect output for all test samples in this fold
                for score,score_fn in scorefn.items():
                    scorei = score_fn(y_test,clf_pred,uy)
                    scores[score].append(scorei)                
                preds.append(clf_pred)
                probs.append(clf_prob)
                models.append(clf)
                modelcvs.append(clf_cv)
                for error,error_fn in errorfn.items():
                    errors[error].append(error_fn(y_test,clf_pred))

            if i==0:
                scorenames = ['%-16s'%score for score in scorekeys]
                logstr = '%-8s %s'%('i',''.join(scorenames))
            else:
                curscores = ['%-16.4f'%(np.mean(scores[score]))
                             for score in scorekeys]                
                logstr = '%-8.3g %s'%(i,''.join(curscores))
            print(logstr,file=logfid,flush=True)

    # train full model for loo cv, score on loo preds from above
    if cv_type == 'loo':
        for score,score_fn in scorefn.items():                
            scores[score] = [score_fn(y,y_pred,uy)]
        for error,error_fn in errorfn.items():
            errors[error] = [error_fn(y,y_pred)]

        clf,clf_cv = train(X,y,clfinputs)
        models = [clf]
        modelcvs = [clf_cv]
        preds = [y_pred]
        probs = [y_prob]
        pbar.update(i+1)
    pbar.finish()    

    # output scores ordered by key
    for score_id in scorekeys:
        score_vals = scores[score_id]
        print('mean %s: %7.4f (std=%7.4f)'%(score_id, np.mean(score_vals),
                                            np.std(score_vals)))

    return {'preds':preds,'probs':probs,'scores':scores,'errors':errors,
            'models':models,'modelcvs':modelcvs}

def trainPredictor(infile):
    process = 'trainPredictor'
    
    # fix the random seed to ensure reproducibility
    np.random.seed(seed=train_state)
    inputs = loadjson(infile)
    outputs = {}
    
    outbase = 'predictor%s'%mdy
    cwd = os.getcwd()    
    try:

        clfinputs = {}
        clfinputs['clf_file'] = inputs['clf_name']+'.pkl'
        clfinputs['clf_type'] = inputs['clf_type']
        clfinputs['classmap'] = loadClassmap(inputs["classmap_file"])
        clfinputs['features'] = loadjson(inputs["feat_file"])        
        
        inputurls     = inputs.pop('urls',[])
        crossvalidate = inputs.pop('crossvalidate',0)
        saveclf       = inputs.pop('saveclf',0)
        cacheoutput   = inputs.pop('cacheoutput',0)    
        
        if not pathexists(outbase):
            os.mkdir(outbase)
        if cacheoutput and not pathexists(pathjoin(outbase,cache_dir)):
            os.mkdir(pathjoin(outbase,cache_dir))        
        os.chdir(outbase)        

    except Exception as e:
        exitv = 10
        message = 'IO Preprocessing failed with exception %s: %s' % (str(e), traceback.format_exc())
        toContext(process,exitv,message)
        sys.exit(1)  
        
    try:
        trdat = collectTrainingData(inputurls,clfinputs,cache=cacheoutput)
        X, y = trdat['X'],trdat['y']
        traintags, trainurls = trdat['traintags'],trdat['trainurls']
        
        errors, skiplist = trdat['skiplist'],trdat['errors']
        print('loaded %d training samples (%d skipped)'%(len(y),len(skiplist)))
    except Exception as e:
        exitv = 11
        message = 'Training data collection failed with exception %s: %s' % (str(e), traceback.format_exc())
        toContext(process,exitv,message)
        sys.exit(1)

    try:
        if crossvalidate:
            cvoutpkl = "cvout.pkl"
            cvlogfile = 'cvout.log'
            print('evaluating model via %s cross-validation (logfile=%s)...'%(cv_type,cvlogfile))
            starttime = time.time()
            cvout = crossValidatePredictor(X,y,clfinputs,logfile=cvlogfile)
            outputs['cv_time'] = time.time()-starttime
            outputs['cv_out'] = cvoutpkl
            outputs['cv_log'] = cvlogfile            
            with open(cvoutpkl,'wb') as fid:
                pickle.dump(cvout,fid)
            print('done, output saved to %s.'%cvoutpkl)
            
    except Exception as e:
        exitv = 12
        message = 'Cross-validation failed with exception %s: %s' % (str(e), traceback.format_exc())
        toContext(process,exitv,message)
        sys.exit(1)
            
    try:
        if saveclf:
            starttime = time.time()
            clf,clfcv = train(X,y,clfinputs)
            clffile = clfinputs['clf_file']
            if clffile[0] != '/': 
                clffile = pathjoin(cwd,clffile) # path relative to cwd
            clfjson = clffile.replace('.pkl','.json')
            outputs['clf_time'] = time.time()-starttime
            outputs['clf_file'] = clffile
 
            print("training classifier using all available data for deployment...")
            with open(clffile,'wb') as fid:
                pickle.dump(clf,fid)
            with open(clfjson,'w') as fid:
                json.dump(clfinputs,fid)
            print('done, output saved to %s.'%clffile)
            
    except Exception as e:
        exitv = 13
        message = 'Classifier training failed with exception %s: %s' % (str(e), traceback.format_exc())
        toContext(process,exitv,message)
        sys.exit(1)

    try:        
        json.dump(outputs,open(outbase+'.met.json','w'),indent=True)
    except Exception:
        os.chdir(cwd)
        exitv = 14
        message = 'Failed to create metadata file for ' + outbase
        toContext(process,exitv,message)
        sys.exit(1)        

    exitv = 0 
    os.chdir(cwd)
    message = 'trainPredictor finished with no errors.'
    toContext(process,exitv,message)            

    
if __name__ == '__main__':
    try: status = trainPredictor(sys.argv[1])
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc()) 
        raise
    sys.exit(status)    
