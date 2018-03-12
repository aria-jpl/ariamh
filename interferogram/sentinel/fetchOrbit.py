#!/usr/bin/env python3

import numpy as np
import requests
import os
import argparse
import datetime
import re
try: from html.parser import HTMLParser
except:
    from HTMLParser import HTMLParser


server = 'https://qc.sentinel1.eo.esa.int/'

orbitMap = [('precise','aux_poeorb'),
            ('restituted','aux_resorb')]

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


class MyHTMLParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.fileList = []
        self.pages = 0
        self.in_td = False
        self.in_a = False
        self.in_ul = False

    def handle_starttag(self, tag, attrs):
        if tag == 'td':
            self.in_td = True
        elif tag == 'a' and self.in_td:
            self.in_a = True
        elif tag == 'ul':
            for k,v in attrs:
                if k == 'class' and v.startswith('pagination'):
                    self.in_ul = True
        elif tag == 'li' and self.in_ul:
            self.pages += 1

    def handle_data(self,data):
        if self.in_td and self.in_a:
            if oper_re.search(data):
                self.fileList.append(data.strip())

    def handle_endtag(self, tag):
        if tag == 'td':
            self.in_td = False
            self.in_a = False
        elif tag == 'a' and self.in_td:
            self.in_a = False
        elif tag == 'ul' and self.in_ul:
            self.in_ul = False
        elif tag == 'html':
            if self.pages == 0:
                self.pages = 1
            else:
                # decrement page back and page forward list items
                self.pages -= 2


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


def fetch(starttime, endtime, mission='S1A', outdir='.', dry_run=False):
    '''
    Determine orbit file to fetch.
    '''

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

        url = server + spec[1]

        query = url + '/?validity_start_time={0}..{1}&mission={2}'.format(timebef, timeaft, mission)

        print(query)
        
        print('Querying for {0} orbits'.format(oType))
        r = session.get(query, verify=False)
        r.raise_for_status()
        parser = MyHTMLParser()
        parser.feed(r.text)
        print("Found {} pages".format(parser.pages))

        results = parser.fileList

        # page through and get more results
        for page in range(2, parser.pages + 1):
            page_query = "{}&page={}".format(query, page)
            print(page_query)
            r = session.get(page_query, verify=False)
            r.raise_for_status()
            page_parser = MyHTMLParser()
            page_parser.feed(r.text)
            results.extend(page_parser.fileList)

        # list all orbit files
        for result in results:
            fields = result.split('_')
            taft = datetime.datetime.strptime(fields[-1][0:15], datefmt)
            tbef = datetime.datetime.strptime(fields[-2][1:16], datefmt)

            # get all files that span the acquisition
            if (tbef <= tstart) and (taft >= tstop):
                tmid = tbef + (taft - tbef)/2
                match.append((os.path.join(url, result),
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
