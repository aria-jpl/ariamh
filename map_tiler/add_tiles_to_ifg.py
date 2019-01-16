#!/usr/bin/env python
"""
Add tiles to IFG product.
"""

import os, sys, json, requests, shutil, argparse
from urlparse import urlparse
from pyes import ES
from pprint import pprint
from subprocess import check_call

from osaka.main import get, put


BASE_PATH = os.path.dirname(__file__)


# get source and destination index
es_url = sys.argv[1]
src = sys.argv[2]
doc_type = "InSAR"

# get connection
conn = ES(es_url)

# index all docs from source index to destination index
query = {
  "query": {
    "match_all": {}
  }
}
r = requests.post('%s/%s/_search?search_type=scan&scroll=60m&size=100' % (es_url, src), data=json.dumps(query))
scan_result = r.json()
count = scan_result['hits']['total']
scroll_id = scan_result['_scroll_id']
results = []
cwd = os.getcwd()
while True:
    r = requests.post('%s/_search/scroll?scroll=60m' % es_url, data=scroll_id)
    res = r.json()
    scroll_id = res['_scroll_id']
    if len(res['hits']['hits']) == 0: break
    for hit in res['hits']['hits']:
        doc = hit['_source']

        # skip if tiles already generated
        tiles = doc['metadata'].get('tiles', False)
        if tiles:
            print "Skipping {}. Tiles already generated.".format(hit['_id'])
            continue

        # create work dir
        prod_id = hit['_id']
        work_dir = prod_id
        prod_url = None
        for url in doc['urls']:
            if url.startswith('s3://'):
                prod_url = url
                break
        if prod_url is None:
            print "Failed to find s3 url for prod %s" % prod_id
            continue
        if os.path.exists(work_dir): shutil.rmtree(work_dir)
        os.makedirs(work_dir, 0755)
        os.chdir(work_dir)
        merged_dir = "merged"
        if os.path.exists(merged_dir): shutil.rmtree(merged_dir)
        os.makedirs(merged_dir, 0755)
        unw_prod_file = "filt_topophase.unw.geo"
        unw_prod_url = "%s/merged/%s" % (prod_url, unw_prod_file)
        get(unw_prod_url, "merged/{}".format(unw_prod_file))
        for i in ('hdr', 'vrt', 'xml'):
            get("{}.{}".format(unw_prod_url, i), "merged/{}.{}".format(unw_prod_file, i))
        
        #print json.dumps(doc, indent=2)

        # clean out tiles if exists
        parsed_url = urlparse(prod_url) 
        tiles_url = "s3://{}/tiles".format(parsed_url.path[1:])
        cmd = "aws s3 rm --recursive {}"
        check_call(cmd.format(tiles_url), shell=True)

        # create displacement tile layer
        vrt_prod_file = "{}.vrt".format(unw_prod_file)
        dis_layer = "interferogram"
        cmd = "{}/create_tiles.py merged/{} {}/{} -b 2 -m prism --nodata 0"
        check_call(cmd.format(BASE_PATH, vrt_prod_file, 'tiles', dis_layer), shell=True)

        # create amplitude tile layer
        amp_layer = "amplitude"
        cmd = "{}/create_tiles.py merged/{} {}/{} -b 1 -m gray --clim_min 10 --clim_max_pct 90 --nodata 0"
        check_call(cmd.format(BASE_PATH, vrt_prod_file, 'tiles', amp_layer), shell=True)

        # upload tiles
        put("tiles", "{}/tiles".format(prod_url))
        
        # upsert new document
        new_doc = {
            "doc": { "metadata": { "tiles": True, "tile_layers": [ amp_layer, dis_layer ] } },
            "doc_as_upsert": True
        }
        r = requests.post('%s/%s/%s/%s/_update' % (es_url, src, doc_type, hit['_id']), data=json.dumps(new_doc))
        result = r.json()
        if r.status_code != 200:
            app.logger.debug("Failed to update user_tags for %s. Got status code %d:\n%s" %
                             (id, r.status_code, json.dumps(result, indent=2)))
        r.raise_for_status()

        # clean
        os.chdir(cwd)
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir)
