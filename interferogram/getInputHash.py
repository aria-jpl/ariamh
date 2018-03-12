#!/usr/bin/env python
import os, sys, json, hashlib
from glob import glob


def getInputHash(netsel_file):
    """Return hash id for the network selector input file."""

    # use sorted list of urls of the InSAR inputs to create hash
    with open(netsel_file) as f:
        netsel_json = json.load(f)
    url_groups = []
    for i in netsel_json:
        urls = [j['url'] for j in i]
        urls.sort()
        url_groups.append(urls)
    m = hashlib.md5()
    input_json = json.dumps(url_groups)
    #print input_json
    m.update(input_json)

    # check for coherenceParams_*
    co_files = glob("coherenceParams_*.json")
    for co_file in co_files:
        with open(co_file) as f:
            m.update(f.read())
    
    # check for pegfile_*
    peg_files = glob("pegfile_*")
    for peg_file in peg_files:
        with open(peg_file) as f:
            m.update(f.read())

    return m.hexdigest()


if __name__ == "__main__":
    hash_id = getInputHash(sys.argv[1])
    with open('netsel_hash.txt', 'w') as f:
        f.write("%s" % hash_id)
