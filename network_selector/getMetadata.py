#!/usr/bin/env python
import os, sys, requests, json, types
from pprint import pprint

from utils.UrlUtils import UrlUtils


def getMetadata(id, output_file):
    """Download metadata json from product repo for product with ID passed in."""

    # get conf settings
    uu = UrlUtils()

    # query
    query = {
        "fields": [ "urls" ],
        "query": {
            "ids": {
                "values": [ id ]
            }
        },
        "filter": {
            "term": {
                "system_version": uu.version
            }
        }
    }

    # get GRQ url
    r = requests.post("%s/%s/_search" % (uu.rest_url, uu.grq_index_prefix),
                      data=json.dumps(query))
    r.raise_for_status()
    res_json = r.json()
    if res_json['hits']['total'] == 0:
        raise RuntimeError("Found no product with id %s." % id)
    res = res_json['hits']['hits'][0]
    urls = res['fields']['urls']
    if not isinstance(urls, types.ListType) or len(urls) == 0:
        raise RuntimeError("Found no urls for product with id %s." % id)
    prod_url = urls[0]

    # get product metadata json
    product = os.path.basename(prod_url)
    met_url = os.path.join(prod_url, '%s.met.json' % product)
    r = requests.get(met_url, auth=(uu.dav_u, uu.dav_p), verify=False)
    r.raise_for_status()
    met_json = r.json()
    with open(output_file, 'w') as f:
        json.dump(met_json, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    getMetadata(sys.argv[1], sys.argv[2])
