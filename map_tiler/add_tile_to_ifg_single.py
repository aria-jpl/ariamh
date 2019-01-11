#!/usr/bin/env python
"""
Add tiles to IFG product.
"""

import os, sys, json, requests, shutil, argparse
from urlparse import urlparse
from pyes import ES
from pprint import pprint
from subprocess import check_call

from osaka.main import get, put, rmall


BASE_PATH = os.path.dirname(__file__)

for name in ["context.json", "_context.json"]:
    try:
        with open(name, "r") as fp:
            context = json.load(fp)
            hit = context["product_hit"]
            break
    except Exception as e:
        print "Failed to find 'product_hit' in metadata from {0}. {1}:{2}".format(name,type(e),e)
else:
    raise Exception("Could not determine product metadata")
try:
    es_url = os.environ["GRQ_URL"]
except Exception as e:
    print "Failed to find GRQ URL from environment. {0}:{1}".format(type(e),e)
    sys.exit(-1)

doc = hit['_source']

# create work dir
prod_id = hit['_id']
work_dir = prod_id
prod_url = None
for url in doc['urls']:
    if prod_url is None:
        prod_url = url
    if url.startswith('s3://'):
        prod_url = url
    if url.startswith("http"):
        print "Browser link: {0}".format(url)

if prod_url is None:
    raise Exception("Failed to find URL for product")
unw_prod_file = "filt_topophase.unw.geo"
unw_prod_url = "%s/merged/%s" % (prod_url, unw_prod_file)
get(unw_prod_url, "merged/{}".format(unw_prod_file))
for i in ('hdr', 'vrt', 'xml'):
    get("{}.{}".format(unw_prod_url, i), "merged/{}.{}".format(unw_prod_file, i))
        
#print json.dumps(doc, indent=2)

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
# clean out tiles if exists
try:
    rmall("{0}/tiles".format(prod_url))
except Exception as e:
    print "Note: could not remove '{0}/tiles'.  May not exit. {1}:{2}".format(prod_url,type(e),e)
put("tiles", "{0}/tiles".format(prod_url))

# upsert new document
new_doc = {
    "doc": { "metadata": { "tiles": True, "tile_layers": [ amp_layer, dis_layer ] } },
    "doc_as_upsert": True
}
r = requests.post('%s/%s/%s/%s/_update' % (es_url, hit["_index"], hit["_type"], hit['_id']), data=json.dumps(new_doc))
result = r.json()
if r.status_code != 200:
    app.logger.debug("Failed to update user_tags for %s. Got status code %d:\n%s" %
                     (id, r.status_code, json.dumps(result, indent=2)))
r.raise_for_status()
