#!/usr/bin/env python3
from utils.queryBuilder import buildQuery, postQuery
import argparse
import os
import sys
import re
from utils.UrlUtils import UrlUtils
import urllib.request, urllib.error, urllib.parse
from html.parser import HTMLParser

# create a subclass and override the handler methods
class MyHTMLParser(HTMLParser):
    def __init__(self,re,nre=[]):
        super().__init__()
        self._re = re
        self._nre = nre
        self._results = []
    @property
    def results(self):
        return self._results
    @results.setter
    def results(self,val):
        self._results = val 
    def handle_starttag(self, tag, attrs):
        pass
    def handle_endtag(self, tag):
        pass
    def handle_data(self, data):
        resnow = None
        for i in self._re:
            found = re.findall(i,data)
            if found:
                resnow = found[0]
                break
        remove = None
        if resnow:
            for i in self._nre:
                remove = re.findall(i,resnow)
                if remove:
                    break
        if not remove and resnow:
            self._results.append(resnow)
            
                        
def getData(args):
    uu = UrlUtils()
     # create a password manager
    meta = {'tags':args.tags,'tag_operator':args.operator}
    ret,status = postQuery(buildQuery(meta))
    try:
        os.mkdir(args.dir)
    except Exception:
        print("directory",args.dir,"already present")
    os.chdir(args.dir)
    for i in ret:
        url = i['url']
        odir = os.getcwd()
        ndir = url.split('/')[-1]
        try:
            os.mkdir(ndir)
        except Exception:
            pass
        os.chdir(ndir)
        for pr in args.products:
            if pr.endswith('.xml'):
                command = 'curl -k -f -u'  + uu.dav_u + ':' + uu.dav_p + ' -O ' + os.path.join(url,pr.replace('.xml',''))
                os.system(command)
            command = 'curl -k -f -u' + uu.dav_u + ':' + uu.dav_p + ' -O ' + os.path.join(url,pr)
            os.system(command)
        
        try:
            password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()

            # Add the username and password.
            password_mgr.add_password(None, urllib.parse.urlparse(url).netloc, uu.dav_u, uu.dav_p)
            
            handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
            # create "opener" (OpenerDirector instance)
            opener = urllib.request.build_opener(handler)
        # use the opener to fetch a URL
            response = opener.open(url).read().decode('utf-8')
        except Exception as e:
            print(e)
        if(response):
            parser = MyHTMLParser(args.re,args.nre)
            parser.feed(response)
            print(parser.results)
            for i in parser.results:
                command = 'curl -k -f -u' + uu.dav_u + ':' + uu.dav_p + ' -O ' + os.path.join(url,i)
                os.system(command)
            
        os.chdir(odir)
def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--nre', type = str, default ='', nargs = '+', dest = 'nre', help = 'noregex')
    parser.add_argument('-r', '--re', type = str, default ='', nargs = '+', dest = 're', help = 'regex')
    parser.add_argument('-t', '--tags', type = str, default ='', nargs = '+', dest = 'tags', help = 'tags')
    parser.add_argument('-p', '--products', type = str, default ='all', nargs = '+', dest = 'products', help = 'what to download')
    parser.add_argument('-o', '--operator', type = str, default ='OR', dest = 'operator', help = 'Logical operator for tags')
    parser.add_argument('-d', '--dir', type = str, default ='./', dest = 'dir', help = 'where to download')
    return parser.parse_args()

def main():
    getData(parse())

if __name__ == '__main__':
    sys.exit(main())
    

