#!/usr/bin/env python
from __future__ import print_function
import re
import os
import sys
import json
import hysds.celery
import shutil
import requests
import math

INPUT = {
        "clf_json":"./clf.json"
    } 

def getURLIndexTypeID(interferogram,version):
    '''
    Returns the ES URL, index, doctype, and id of the given interferogram
    @param interferogram - interferogram to search for
    '''
    es_url = hysds.celery.app.conf.GRQ_ES_URL
    es_index = "grq"
    _id = interferogram.replace("interferogram_","interferogram__")
    #if not _id.startswith("interferogram") and not _id.endswith(version):
    #    _id = _id + "-"+version 
    query = {
        "query": {
            "bool": {
                "must": [
                    { "term": { "_id": _id  }  }
                ]
            }
        }
    }
    url = "%s/%s/_search/" % (es_url, es_index)
    data=json.dumps(query,indent=2)
    print("Posting ES search: {0} with {1}".format(url,data))
    r = requests.post(url,data=data)
    r.raise_for_status()
    print("Got: {0}".format(r.json()))
    result = r.json()
    if len(result["hits"]["hits"]) == 0:
         raise Exception("Interferogram not found in ES index: {0}".format(interferogram))
    elif len(result["hits"]["hits"])  > 1:
         raise Exception("Interferogram found multiple times: {0}".format(interferogram))
    return (es_url,result["hits"]["hits"][0]["_index"],result["hits"]["hits"][0]["_type"],result["hits"]["hits"][0]["_id"])
def updateDocument(interferogram,version,prediction,label):
    '''
    Update the ES document with new information
    Note: borrowed from user_tags
    @param interferogram - interferogram to stamp with prediction
    @param prediction - prediction to stamp to interferogram
    '''
    new_doc = {
        "doc": {
            "predicted_phase_unwrapping_quality": prediction,
            "metadata":{
                "predicted_phase_unwrapping_quality": prediction,
                "predicted_label":{
                    "UWE":label
                },
                "predicted_phase_unwrapping_bucket":int(math.floor(float(prediction)/10)*10)
            }
        },
        "doc_as_upsert": True
    }
    url = "{0}/{1}/{2}/{3}/_update".format(*getURLIndexTypeID(interferogram,version))
    print("Updating: {0} with {1}".format(url,new_doc))
    r = requests.post(url, data=json.dumps(new_doc))
    r.raise_for_status()

if __name__ == "__main__":
    '''
    Main program for the wrapper
    V2 - compliant
    '''
    ret = 0
    if len(sys.argv) != 2:
        print("Features URL not provided",file=sys.stderr)
        sys.exit(-1)
    models = [directory for directory in os.listdir(".") if os.path.isdir(os.path.join(".",directory)) and directory.startswith("predictor_")]
    if len(models) != 1:
        print("Invalid number of predictor models: {0}".format(len(models)),file=sys.stderr)
        sys.exit(-1)
    model = models[0]
    INPUT["feat_url"] = sys.argv[1]
    if INPUT["feat_url"].startswith("interferogram"):
        INPUT["feat_url"] = INPUT["feat_url"].replace("interferogram","features_interferogram")
    with open("./input.json","w") as f:
        json.dump(INPUT,f)
    CLF = {}
    CLF["feat_file"] = os.path.join(".",model,"featv3.0.json")
    CLF["clf_file"] = os.path.join(".",model,model+".pkl")
    with open("./clf.json","w") as f:
        json.dump(CLF,f)
    ret = os.system("ARIAMH_HOME=/home/ops/ariamh/ PYTHONPATH=/home/ops/ariamh/ /home/ops/ariamh/ariaml/runPredictor.py ./input.json")
    #Find the prediciton and use it
    preds = []
    for item in os.listdir("."):
        print("Checking item: ",item)
        if os.path.isdir(item) and "validation_" in item:
            preds.append(item)
    if len(preds) == 0:
        raise Exception("No prediction product created")
    elif len(preds) > 1:
        raise Exception("Too many prediction products created")
    #Dump the product and harvest its information. Then post to ES
    with open(os.path.join(".",preds[0],preds[0]+".met.json"),"r") as jsn:
        product = json.load(jsn)
        try:
            f = INPUT["feat_url"].strip("/").split("/")[-1]+".met.json"
            with open(f,"r") as fl:
                version = json.load(fl)["interferogram_version"]
        except Exception as e:
            print("Failed to get version: {0}:{1}".format(type(e),e),file=sys.stderr)
            version = "v1.0"
        updateDocument(re.sub("_\d{4}-\d{2}-\d{2}T\d{6}.\d{6}","",product["orig_prod"]),version,int(round(float(product["prob"])*100)),product["pred_lab"])
    #We ingest no products, so remove the met.json
    for directory in [item for item in os.listdir(".") if os.path.isdir(item)]:
        torm = os.path.join(".",directory,directory+".met.json") 
        print("Moving .met.json to prevent re-ingest of localized products: {0}".format(directory))
        try:
             shutil.move(torm,torm+".noingest.json")
        except:
             print("Failed to move: {0} safely. May not exist.".format(torm))
        #Datasets
        torm = os.path.join(".",directory,directory+".dataset.json") 
        print("Moving .dataset.json to prevent re-ingest of localized products: {0}".format(directory))
        try:
            shutil.move(torm,torm+".noingest.json")
        except:
             print("Failed to move: {0} safely. May not exist.".format(torm))
    sys.exit(ret)
