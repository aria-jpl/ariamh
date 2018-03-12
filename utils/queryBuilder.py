#! /usr/bin/env python3 

import os
import sys
import json
import requests
from pprint import pprint
from utils.UrlUtils import UrlUtils
from datetime import datetime, timedelta
try:
    from frameMetadata.FrameMetadata import FrameMetadata
except ImportError:
    print("createMetaObjects method cannot be used")
    
listNotMeta = ['city.country_name.raw','system_version.raw','dataset_type','dataset_type.raw','dataset','dataset_level','city.admin1_name.raw']
#list_untouched = ['sensor','city.admin1_name.raw']
list_untouched = ['sensor']

def getIndexAndUrl(sv = '', conf = ''):
    uu = UrlUtils(conf)
    if not sv:
        sv = uu.version
    return uu.grq_index_prefix + '_' + sv + '_*', uu.rest_url

def getRangeDate(meta,key):
    fmt = '%Y-%m-%dT%H:%M:%SZ'
    dt = datetime.strptime(meta[key],fmt)
    sensingMin = dt - timedelta(seconds = 1)
    sensingMax = dt + timedelta(seconds = 1)
    del meta[key]
    return {'range':{'metadata.' + key:{'lte':sensingMax.strftime(fmt),'gte':sensingMin.strftime(fmt)}}}

def getRangeLat(meta,option):
    if(not 'latitudeIndexMin' in meta or not 'latitudeIndexMax' in meta):
        print('latitudeIndexMin and latitudeIndexMax must be present in the metadata for option ', option)
        raise Exception
    latitudeIndexMin = float(meta['latitudeIndexMin'])
    latitudeIndexMax = float(meta['latitudeIndexMax'])
    if(option == 'within'):
        latimin = {'gte':latitudeIndexMin}
        latimax = {'lte':latitudeIndexMax}
    elif(option == 'cross-boundaries'):
        latimin = {'lte':latitudeIndexMax}
        latimax = {'gte':latitudeIndexMin}
    del meta['latitudeIndexMin']
    del meta['latitudeIndexMax']
    return [{'range':{'metadata.latitudeIndexMin':latimin}},{'range':{'metadata.latitudeIndexMax':latimax}}]

def getRangeOrbit(meta,options):
    if ('orbitNumber' not in meta) and ('orbitRepeat' not in meta) and ('num_repeats' not in meta):
        print('orbitNumber and orbitRepeat must be in meta and num_repeats in options')
        raise
    nrep = meta['num_repeats']
    orep = meta['orbitRepeat']
    onum = meta['orbitNumber']
    del meta['orbitRepeat']
    del meta['orbitNumber']
    del meta['num_repeats']
    #get all the orbits greater or equal the orbit number minus nrep times the
    #orbit repeat. also get all the ones less then the current orbit.
    #this is used to find slaves that come before the master. Since for constellations
    #one might have acquisitions in between orbit repeats from other platform
    #we don't check for == onum - nrep*orep, but also evertyhing that came
    #after
    return [{'range':{'metadata.orbitNumber':{'gte':onum - nrep*orep}}},
             {'range':{'metadata.orbitNumber':{'lt':onum}}}]

def getTerms(meta):
    retList = []
    for k,v in list(meta.items()):
        namespace = ''
        if not listNotMeta.count(k):
            namespace = 'metadata.'
        extra = ''
        if k in list_untouched:
            extra = '.untouched'
        retList.append({'term':{ namespace + k + extra:v}}) 
    return retList
def getTags(meta):
    retQuery = {}
    if 'tags' in meta:
        if 'tag_operator' in meta:
            operator = meta['tag_operator']
            del meta['tag_operator']
        else:
            operator = "OR"
        tags = meta['tags']
        del meta['tags']
        if isinstance(tags,str):
            tags = [tags]
        queryStr = ''
        toAppend = ''
        for tag in tags:
            queryStr += toAppend + '\"' + tag + '\"'
            toAppend = '|'
        retQuery = {"query_string":{"query":queryStr,"default_operator":operator}}
    return retQuery

     
def getFilter(meta,options):
   
    #check if sensing start is present/
    #need to create a small range around the value
    andList = []
    if 'sensingStart' in meta:
        andList.append(getRangeDate(meta,'sensingStart'))
    if 'sensingStop' in meta:
        andList.append(getRangeDate(meta,'sensingStop'))
    if options.count('within'):
        andList.extend(getRangeLat(meta,'within'))
    elif options.count('cross-boundaries'):
        andList.extend(getRangeLat(meta,'cross-boundaries'))
    if (('orbitNumber' in meta) and ('orbitRepeat' in meta)
        and ('num_repeats' in meta)):
        andList.extend(getRangeOrbit(meta, options))
    if meta:
        andList.extend(getTerms(meta))
    return andList
        
#pass all the data in a dict and define the different options in a list (like search within, cross-boundaries) 
def buildQuery(metain,options = None):
    if options is None:
        options = []
    import copy
    meta = copy.deepcopy(metain)
    
    query = \
{
  "sort": 
  {
    "_id":
     {
      "order": "desc"
     }
  },
  "fields":
   [
    "_timestamp",
    "_source"
   ],
    "query":
    {
    "match_all": {}
    }
} 
    tags = getTags(meta)
    if tags:
        query['query'] = tags
    #query['query'] = {"query_string":{"query":'UWE',"default_operator":"OR"}}        
    filter = getFilter(meta,options)
    if filter:
        query['filter'] = {'and':filter}
    return query

def removeDuplicates(qlist):
    res = {}
    for j,i in enumerate(qlist):
        if not i['url'] in res:
            res[i['url']] = j
    retList = []
    for k,v in res.items():
        retList.append(qlist[v])
    return retList
        
    
      
def postQuery(query,sv='',conf=''):
    #r = requests.post('%s/%s/_search?search_type=scan&scroll=10m&size=100' % (es_url, index), data=json.dumps(query))
    index,es_url = getIndexAndUrl(sv,conf)
    #r = requests.post('%s/%s/_search?search_type=scan&scroll=10m&size=100' % (es_url, index), data=query)
    r = requests.post('%s/%s/_search?search_type=scan&scroll=10m&size=100' % (es_url, index), data=json.dumps(query))
    status = True
    retList = []
    if r.status_code != 200:
        status = False
    else:
        scan_result = r.json()
        #print("scan_result: {}".format(json.dumps(scan_result, indent=2)))
        if '_scroll_id' in scan_result:
            scroll_id = scan_result['_scroll_id']
            while True:
                r = requests.post('%s/_search/scroll?scroll=10m' % es_url, data=scroll_id)
                res = r.json()
                scroll_id = res['_scroll_id']
                if len(res['hits']['hits']) == 0: break
                for hit in res['hits']['hits']:
                    #url is not part of the metadata, so add it
                    hit['_source']['metadata']['url'] = hit['_source']['urls'][0]
                    retList.append(hit['_source']['metadata'])
                    hit['_source']['metadata']['id'] = hit['_source']['id']
    if status:
        retList = removeDuplicates(retList)
    return retList,status
def createMetaObjects(metaList):
    retList = []
    for meta in metaList:
        fm = FrameMetadata()
        fm.load(meta)
        retList.append(fm)
    return retList
    
def main():
    options = []
    #meta = {'platform':'csk','sensingStart':'2011-06-28T13:57:06Z','trackNumber':200,'latitudeIndexMin':370,'latitudeIndexMax':379,'direction':'asc'}
    #meta = {'system_version':'v0.3','platform':'csk','trackNumber':164,'latitudeIndexMin':189,'latitudeIndexMax':199,'lookDirection': 'right','direction':'dsc','beamID':'H4-10'}
    #meta = {'beamID': u'H4-10', 'direction': u'dsc', 'latitudeIndexMin': 193, 'platform': u'csk', 'orbitNumber': 27656, 'system_version': 'v0.3', 'latitudeIndexMax': 195}
    meta = {'dataset_type':'raw'}
    #query = {"query":{"bool":{"must":[{"term":{"system_version":"v1.0"}},{"term":{"metadata.beamID":"H4-18"}},
    #        {"term":{"metadata.trackNumber":"44"}},{"term":{"dataset":"interferogram"}},{"term":{"metadata.latitudeIndexMin":"342"}}]}},
    #         "sort":[{"_timestamp":{"order":"desc"}}],"fields":["_timestamp","_source"]}
    ret,status = postQuery(buildQuery(meta,options))
    #ret,status = postQuery(query)

    #retm = createMetaObjects(ret)
    #print(len(ret))
    #print(status)
    cnt = 0
    for val in ret:
        if not val['url']:
            cnt += 1
    #print(len(ret),cnt,status)  
        #print(val['reference'],val['trackNumber'],val['latitudeIndexMin'],val['latitudeIndexMax'],val['beamID'],val['sensingStart'])
    #pass
if __name__ == '__main__':
    sys.exit(main())
