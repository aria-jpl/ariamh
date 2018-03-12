#!/usr/bin/env python
import os, sys, requests, json
from pprint import pprint

from utils.UrlUtils import UrlUtils


def check_int(es_url, es_index, hash_id):
    """Query for interferograms with specified input hash ID."""

    query = {
        "query":{
            "bool":{
                "must":[
                    {"term":{"metadata.input_hash_id":hash_id}},
                ]
            }
        }
    }

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code != 200:
        print >>sys.stderr, "Failed to query %s:\n%s" % (es_url, r.text)
        print >>sys.stderr, "query: %s" % json.dumps(query, indent=2)
        print >>sys.stderr, "returned: %s" % r.text
    r.raise_for_status()
    result = r.json()
    pprint(result)
    total = result['hits']['total']
    if total == 0: id = 'NONE'
    else: id = result['hits']['hits'][0]['_id']
    return total, id


if __name__ == "__main__":
    uu = UrlUtils()
    es_url = uu.rest_url
    es_index = '%s_interferogram' % uu.grq_index_prefix
    total, id = check_int(es_url, es_index, sys.argv[1])
    with open('interferograms_found.txt', 'w') as f:
        f.write("%d\n%s\n" % (total, id))
