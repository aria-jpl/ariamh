#!/usr/bin/env python3
import os, sys, re, json, requests, datetime, tarfile, argparse
from pprint import pprint
import numpy as np

from utils.UrlUtils import UrlUtils


server = 'https://qc.sentinel1.eo.esa.int/'

cal_re = re.compile(r'S1\w_AUX_CAL')

def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser(description='Fetch calibration auxiliary files ingested into HySDS')
    parser.add_argument('-o', '--output', dest='outdir', type=str, default='.',
            help='Path to output directory')
    parser.add_argument('-d', '--dry-run', dest='dry_run', action='store_true',
            help="Don't download anything; just output the URLs")

    return parser.parse_args()


def download_file(url, outdir='.', session=None):
    '''
    Download file to specified directory.
    '''

    if session is None:
        session = requests.session()

    path = "%s.tgz" % os.path.join(outdir, os.path.basename(url))
    print('Downloading URL: ', url)
    request = session.get(url, stream=True, verify=False)
    request.raise_for_status()
    with open(path,'wb') as f:
        for chunk in request.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()
    return path


def untar_file(path, outdir):
    '''
    Extract aux cal files.
    '''

    if not tarfile.is_tarfile(path):
        raise RuntimeError("%s is not a tarfile." % path)
    with tarfile.open(path) as f:
        f.extractall(outdir)


def get_active_ids(es_url):
    """Query for the active calibration IDs."""

    query = {
        "query":{
            "bool":{
                "must":[
                    {"term":{"_id": "S1_AUX_CAL_ACTIVE"}},
                ]
            }
        },
        "sort":[ { "starttime": { "order": "desc" } } ]
    }

    es_index = "grq_*_s1-aux_cal_active"
    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code == 200:
        result = r.json()
        #pprint(result)
        total = result['hits']['total']
        if total == 0:
            raise RuntimeError("Failed to find S1_AUX_CAL_ACTIVE at %s." % search_url)
        return result['hits']['hits'][0]['_source']['metadata']['active_ids']
    else:
        print("Failed to query %s:\n%s" % (es_url, r.text), file=sys.stderr)
        print("query: %s" % json.dumps(query, indent=2), file=sys.stderr)
        print("returned: %s" % r.text, file=sys.stderr)
        r.raise_for_status()


def get_cal_url(id, es_url):
    """Query for the active calibration url."""

    query = {
        "query":{
            "bool":{
                "must":[
                    {"term":{"_id": id}},
                ]
            }
        },
        "fields": ["urls", "metadata.archive_filename"]
    }

    es_index = "grq_*_s1-aux_cal"
    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code == 200:
        result = r.json()
        pprint(result)
        total = result['hits']['total']
        if total == 0:
            raise RuntimeError("Failed to find %s at %s." % (id, search_url))
        urls = result['hits']['hits'][0]['fields']['urls']
        archive_fname = result['hits']['hits'][0]['fields']['metadata.archive_filename'][0]
        url = next(filter(lambda x: x.startswith('http'), urls))
        #print(urls)
        #print(url)
        #print(archive_fname)
        return os.path.join(url, archive_fname)
    else:
        print("Failed to query %s:\n%s" % (es_url, r.text), file=sys.stderr)
        print("query: %s" % json.dumps(query, indent=2), file=sys.stderr)
        print("returned: %s" % r.text, file=sys.stderr)
        r.raise_for_status()


def fetch(outdir, dry_run):

    # get endpoint configurations
    uu = UrlUtils()
    es_url = uu.rest_url

    # get active calibration ids
    active_ids = get_active_ids(es_url)
    print(active_ids)

    # get urls for active calibration files
    cal_urls = [get_cal_url(i, es_url) for i in active_ids]
    print(cal_urls)


    if len(cal_urls) == 0:
        print('Failed to find calibration auxiliary files')


    if dry_run: print('\n'.join(cal_urls))
    else:
        if not os.path.isdir(outdir): os.makedirs(outdir)
        for cal_url in cal_urls:
            try: cal_file = download_file(cal_url, outdir)
            except:
                print('Failed to download URL: ', cal_url)
                raise
            try: cal_dir = untar_file(cal_file, outdir)
            except:
                print('Failed to untar: ', cal_file)
                raise
            os.unlink(cal_file)


if __name__ == '__main__':
    inps = cmdLineParse()
    fetch(inps.outdir, inps.dry_run)
