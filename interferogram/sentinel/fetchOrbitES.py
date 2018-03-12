#!/usr/bin/env python3

import os, sys, re, json, requests, datetime, tarfile, argparse
from pprint import pprint, pformat
import numpy as np

from utils.UrlUtils import UrlUtils


server = 'https://qc.sentinel1.eo.esa.int/'

orbitMap = [('precise','S1-AUX_POEORB'),
            ('restituted','S1-AUX_RESORB')]

misMap = { 'S1A': 'Sentinel-1A',
           'S1B': 'Sentinel-1B' }

datefmt = "%Y%m%dT%H%M%S"
queryfmt = "%Y-%m-%d"

oper_re = re.compile(r'S1\w_OPER')

def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser(description='Fetch orbits corresponding to given sensing start and end time')
    parser.add_argument('-s', '--starttime', dest='starttime', type=str, required=True,
            help='sensing start time')
    parser.add_argument('-e', '--endtime', dest='endtime', type=str, required=True,
            help='sensing stop time')
    parser.add_argument('-m', '--mission', dest='mission', type=str, default='S1A',
            help='mission (S1A or S1B)')
    parser.add_argument('-o', '--output', dest='outdir', type=str, default='.',
            help='Path to output directory')
    parser.add_argument('-d', '--dry-run', dest='dry_run', action='store_true',
            help="Don't download anything; just output the URL")

    return parser.parse_args()


def download_file(url, outdir='.', session=None):
    '''
    Download file to specified directory.
    '''

    if session is None:
        session = requests.session()

    path = os.path.join(outdir, os.path.basename(url))
    print('Downloading URL: ', url)
    request = session.get(url, stream=True, verify=False)

    try:
        val = request.raise_for_status()
        success = True
    except:
        success = False

    if success:
        with open(path,'wb') as f:
            for chunk in request.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()

    return success


def get_orbits(es_url, otype, timebef, timeaft, mission):
    """Query for the orbits."""

    query = {
        "query":{
            "bool":{
                "must":[
                    {
                        "term": {
                            "metadata.platform.raw": misMap[mission]
                        }
                    },
                    {
                        "range": {
                            "starttime": {
                                "from": timebef,
                                "to": timeaft
                            }
                        }
                    }
                ]
            }
        },
        "fields": ["urls", "metadata.archive_filename"],
        "sort":[ { "metadata.creationTime": { "order": "desc" } } ]
    }

    es_index = "grq_*_%s" % otype.lower()
    if es_url.endswith('/'): es_url = es_url[:-1]
    search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post("%s?search_type=scan&scroll=60&size=100" % search_url, data=json.dumps(query))
    if r.status_code == 200:
        scan_result = r.json()
        total = scan_result['hits']['total']
        if total == 0:
            print("Failed to find %s orbit at %s for: %s" % (otype, search_url, json.dumps(query, indent=2)))
            return []
        scroll_id = scan_result['_scroll_id']
        results = []
        while True:
            r = requests.post('%s/_search/scroll?scroll=60m' % es_url, data=scroll_id)
            res = r.json()
            scroll_id = res['_scroll_id']
            if len(res['hits']['hits']) == 0: break
            results.extend(res['hits']['hits'])
        return results
    else:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()


def fetch(starttime, endtime, mission='S1A', outdir='.', dry_run=False):
    '''
    Determine orbit file to fetch.
    '''

    # get endpoint configurations
    uu = UrlUtils()
    es_url = uu.rest_url

    tfmt = "%Y-%m-%dT%H:%M:%S.%f"
    tstart = datetime.datetime.strptime(starttime, tfmt)
    tstop = datetime.datetime.strptime(endtime, tfmt)
    timeStamp = tstart + (tstop - tstart)/2

    match = []
    bestmatch = None
    session = requests.Session()
    for spec in orbitMap:
        oType = spec[0]

        if oType == 'precise':
            delta = datetime.timedelta(days=2)
        elif oType == 'restituted':
            delta = datetime.timedelta(days=1)

        timebef = (timeStamp - delta).strftime(queryfmt)
        timeaft = (timeStamp + delta).strftime(queryfmt)

        results = get_orbits(es_url, spec[1], timebef, timeaft, mission)
        #print(results)
        
        # list all orbit files
        for res in results:
            urls = res['fields']['urls']
            archive_fname = res['fields']['metadata.archive_filename'][0]
            filtered = filter(lambda x: x.startswith('http'), urls)
            if isinstance(filtered, list): url = filtered[0]
            else: url = next(filtered)
            fields = archive_fname.split('_')
            taft = datetime.datetime.strptime(fields[-1][0:15], datefmt)
            tbef = datetime.datetime.strptime(fields[-2][1:16], datefmt)

            # get all files that span the acquisition
            if (tbef <= tstart) and (taft >= tstop):
                tmid = tbef + (taft - tbef)/2
                match.append((os.path.join(url, archive_fname),
                              abs((timeStamp-tmid).total_seconds())))

        # return the file with the image is aligned best to the middle of the file
        if len(match) != 0:
            bestmatch = min(match, key = lambda x: x[1])[0]
            break
        else:
            print('Failed to find {0} orbits for Time {1}'.format(oType, timeStamp))

    if bestmatch:
        if dry_run: print(bestmatch)
        else:
            res = download_file(bestmatch, outdir, session=session)
            if res is False:
                print('Failed to download URL: ', bestmatch)

    session.close()

    return bestmatch


if __name__ == '__main__':
    inps = cmdLineParse()
    fetch(inps.starttime, inps.endtime, inps.mission, inps.outdir, inps.dry_run)
