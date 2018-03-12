#!/usr/bin/env python3
from ariaml.Predictor import Predictor
from utils.contextUtils import toContext
from utils.UrlUtils import UrlUtils

import json
import sys
import os
import traceback

def main():
    process = 'validation'
    try:
        inputs = json.load(open(sys.argv[1]))
        clf_json = inputs['clf_json']
        url = inputs['feat_url']
        #otherwise prdbase gets messed up
        if(url.endswith('/')):
            url = url[:-1]   
        uu = UrlUtils()
        urlsplit = url.split('/')
        if urlsplit[-2].startswith("feature"):
            feat_json = (urlsplit[-2] + '_' + urlsplit[-1]).replace('__','_')  + '.met.json'
        else:
            feat_json = urlsplit[-1] + ".met.json"

        print(url,urlsplit,feat_json)
        try:
            command = 'curl -k -f -u' + uu.dav_u + ':' + uu.dav_p + ' -O ' + os.path.join(url,feat_json)
            os.system(command)
        except Exception:
            exitv = 11
            message = 'Failed to download metadata for ' + prdbase
            toContext(process,exitv,message)
            sys.exit(1)        
        product = feat_json.replace('features_','validation_').replace('.met.json','')
        p = Predictor(clf_json)
        pred,lab = p.predict(feat_json)[0]
        try:
            os.mkdir(product)
        except Exception:
            pass
        cwd = os.getcwd()
        os.chdir(product)
        res = {'prob':pred,'pred_lab':lab,'orig_prod':feat_json.replace('features_','').replace('.met.json','')}
        #for provenance add the inputs used for the run 
        toAdd = json.load(open(os.path.join(cwd,clf_json)))
        res.update(toAdd)
        #move the feat_file to the product dir so it gets picked up
        os.system('mv ' + os.path.join('..',toAdd['feat_file']) + ' ./')
        json.dump(res,open(product+'.met.json','w'),indent=True)
        
    except Exception as e:
        os.chdir(cwd)
        exitv = 10
        message = 'Failed with exception %s: %s' % (str(e), traceback.format_exc())
        toContext(process,exitv,message)
        sys.exit(1)        
    
    os.chdir(cwd)
    exitv = 0
    message = 'Validation finished with no errors.'
    toContext(process,exitv,message)
if __name__ == '__main__':
    sys.exit(main())
   
