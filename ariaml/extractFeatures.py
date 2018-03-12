#!/usr/bin/env python3
from ariaml.FeaturesExtractor import FeaturesExtractor as FE
import json
import sys
import os
import os.path
import re
import traceback
import datetime
from utils.UrlUtils import UrlUtils
from utils.contextUtils import toContext

def extractFeatures(infile):
    process = 'extractFeatures'
    try:
        inputs = json.load(open(infile))
        url = inputs['url']
        #otherwise prdbase gets messed up
        if(url.endswith('/')):
            url = url[:-1]   
        urlsplit = url.split('/')
        #need to be consistent with the naming convention bur we are not
        if(url.count('CSK')):  
            prdbase = (urlsplit[-2] + '_' + urlsplit[-1]).replace('__','_')
        elif (url.count('S1')):
            prdbase = urlsplit[-1]
        product = 'features_' + prdbase
        fe = FE(url,product)
        res = fe.extractFeatures()

    except Exception as e:
        exitv = 10
        message = 'Failed with exception %s: %s' % (str(e), traceback.format_exc())
        toContext(process,exitv,message)
        sys.exit(1)        
    #Get the default version
    try:
        version = re.search(r"/(v[^/]+)/",url).group(1)
    except Exception as e2:
        print("Failed to get version from URL. Using extractor product version. {0}.{1}".format(type(e2),e2))
        version = "v1.0"
    try:
        os.mkdir(product)
    except Exception:
        pass
    cwd = os.getcwd()
    os.chdir(product)
    uu = UrlUtils()
    try:
        command = 'curl -k -f -u' + uu.dav_u + ':' + uu.dav_p + ' -O ' + os.path.join(url,prdbase  + '.met.json')
        print(command)
        os.system(command)
    except Exception:
        os.chdir(cwd)
        exitv = 11
        message = 'Failed to download metadata for ' + prdbase
        toContext(process,exitv,message)
        sys.exit(1)
    if not os.path.exists(prdbase  + '.met.json'):
        try:
            newdl = prdbase.replace("-"+version,"")
            print("Met JSON not found, attempting to grab:",newdl)
            command = 'curl -k -f -u' + uu.dav_u + ':' + uu.dav_p + ' -O ' + os.path.join(url, newdl  + '.met.json')
            print(command)
            os.system(command)
        except Exception:
            pass
    try:
        command = 'curl -k -f -u' + uu.dav_u + ':' + uu.dav_p + ' -O ' + os.path.join(url,prdbase  + '.dataset.json')
        print(command)
        os.system(command)
    except Exception as e:
        print("Failed to download datasets.json. Ignoring. {0}.{1}".format(type(e),e))
    try:
        toAdd = json.load(open(prdbase  + '.met.json'))
        for key in ['orbit','tags','inputFile','input_has_id',
                    'product_type','dataset_type','orbitNumber']:
            try:#no use for orbit
                del toAdd[key]
            except Exception:
                pass
        res.update(toAdd)
        os.remove(prdbase  + '.met.json')
        try:
            dset = json.load(open(prdbase  + '.dataset.json'))
            version = dset["version"]
            os.remove(prdbase  + '.dataset.json')
        except Exception as e:
            print("Failed to get version from dataset. Using URL. {0}.{1}".format(type(e),e))
        res["interferogram_version"] = version
        res["interferogram_id"] = dset.get("label", prdbase)
        with open(product+'.met.json','w') as fp:
            json.dump(res,fp,indent=True)
        dset["creation_timestamp"] = datetime.datetime.now().isoformat()
        dset["label"] = product
        with open(os.path.join(os.path.dirname(__file__),"..","conf","dataset_versions.json"), "r") as fp:
            dset["version"] = json.load(fp).get("features","v1.0")
        with open(product+'.dataset.json','w') as fp:
            json.dump(dset,fp,indent=True)
    except Exception as e:
        print("[ERROR] Exception occured. {0}:{1}\n{2}".format(e,type(e),traceback.format_exc()))
        os.chdir(cwd)
        exitv = 12
        message = 'Failed create metadata file for ' + product
        toContext(process,exitv,message)
        sys.exit(1)
    exitv = 0 
    os.chdir(cwd)
    message = 'Extract features finished with no errors.'
    toContext(process,exitv,message)
def main():
    extractFeatures(sys.argv[1])

if __name__ == '__main__':
    try: status = main()
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc()) 
        raise
    sys.exit(status)
