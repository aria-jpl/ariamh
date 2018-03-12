#!/usr/bin/env python
import os, sys, requests, json
from kombu import Connection
from datetime import datetime
from pprint import pprint


QUERY1 = {
    "query":{
        "bool":{
            "must":[
                {"term":{"dataset":"CSK"}},
                {"term":{"metadata.direction":"asc"}},
                {"term":{"metadata.trackNumber":"200"}},
                {"term":{"metadata.latitudeIndexMax":"377"}}
            ]
        }
    },
    "sort":[{"_timestamp":{"order":"desc"}}]
}


QUERY2 = {
    "query":{
        "bool":{
            "must":[
                {"term":{"dataset":"CSK"}},
                {"term":{"metadata.direction":"dsc"}},
                {"term":{"metadata.trackNumber":"74"}},
                {"term":{"metadata.beamNumber":"01"}},
                {
                    "query_string":{
                        "query":"latitudeIndexMin:[369 TO 371]",
                        "default_operator":"OR"
                    }
                }
            ]
        }
    },
    "sort":[{"_timestamp":{"order":"desc"}}]
}


def get_job(objectid):
    """Return json job configuration for NS/CI."""

    return {
        "job_type": "job:ariamh_sciflo_create_interferogram",
        "payload": {
            "objectid": objectid,
        }
    } 


def submit_jobs(es_url, es_index, mozart_url, job_queue, done_json, query):
    """Query all CSK scenes for California and submit NS/CI jobs."""

    done_dict = {}
    if os.path.exists(done_json):
        with open(done_json) as f:
            done_dict = json.load(f)

    search_url = '%s/%s/_search?search_type=scan&scroll=10m&size=100' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code != 200:
        print >>sys.stderr, "Failed to query %s:\n%s" % (es_url, r.text)
        print >>sys.stderr, "query: %s" % json.dumps(query, indent=2)
        print >>sys.stderr, "returned: %s" % r.text
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    #pprint(scan_result)
    submitted = 0
    with Connection(mozart_url) as conn:
        conn.ensure_connection()
        with conn.SimpleQueue(job_queue) as queue:
            while True:
                r = requests.post('%s/_search/scroll?scroll=10m' % es_url, data=scroll_id)
                res = r.json()
                scroll_id = res['_scroll_id']
                #pprint(res)
                if len(res['hits']['hits']) == 0: break
                for hit in res['hits']['hits']:
                    id = hit['_id']
                    if id in done_dict: continue
                    job_json = get_job(id)
                    queue.put(json.dumps(job_json))
                    done_dict[id] = True
                    submitted += 1
    print "Submitted %s jobs for NS/CI workflows over California." % submitted

    with open(done_json, 'w') as f:
        json.dump(done_dict, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    es_url = 'http://aria-products.jpl.nasa.gov:9200'
    es_index = 'grq_csk'
    mozart_url = 'amqp://guest:guest@aria-jobs.jpl.nasa.gov:5672//'
    job_queue = 'jobs_processed'
    submit_jobs(es_url, es_index, mozart_url, job_queue, 'cali_jobs.json', QUERY1)
    submit_jobs(es_url, es_index, mozart_url, job_queue, 'cali_jobs.json', QUERY2)
