#!/usr/bin/env python3

import numpy as np
import requests
import os
import argparse
import datetime
import tarfile
import re
try: from html.parser import HTMLParser
except:
    from HTMLParser import HTMLParser


server = 'https://qc.sentinel1.eo.esa.int/'

cal_re = re.compile(r'S1\w_AUX_CAL')

def cmdLineParse():
    '''
    Command line parser.
    '''

    parser = argparse.ArgumentParser(description='Fetch calibration auxiliary files')
    parser.add_argument('-o', '--output', dest='outdir', type=str, default='.',
            help='Path to output directory')
    parser.add_argument('-d', '--dry-run', dest='dry_run', action='store_true',
            help="Don't download anything; just output the URLs")

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
            if cal_re.search(data):
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


def fetch(outdir, dry_run):

    session = requests.Session()
    url = server + 'aux_cal'
    query = url + '/?active=True'
    print(query)
    cal_urls = []
    try:
        print('Querying for active calibration auxiliary files')
        r = session.get(query, verify=False)
        r.raise_for_status()
        #print(r.text)
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

        for result in results:
            cal_urls.append(os.path.join(url, result))
    except :
        pass

    if len(cal_urls) == 0:
        print('Failed to find calibration auxiliary files')


    if dry_run: print('\n'.join(cal_urls))
    else:
        if not os.path.isdir(outdir): os.makedirs(outdir)
        for cal_url in cal_urls:
            try: cal_file = download_file(cal_url, outdir, session=session)
            except:
                print('Failed to download URL: ', cal_url)
                raise
            try: cal_dir = untar_file(cal_file, outdir)
            except:
                print('Failed to untar: ', cal_file)
                raise
            os.unlink(cal_file)

    session.close()


if __name__ == '__main__':
    inps = cmdLineParse()
    fetch(inps.outdir, inps.dry_run)
