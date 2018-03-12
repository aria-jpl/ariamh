#!/usr/bin/env python
import os, sys, requests
from urlparse import urlparse
from StringIO import StringIO
from lxml.etree import parse
from easywebdav import connect


def get_remote_dav(url):
    """Get remote dir/file."""

    lpath = './%s' % os.path.basename(url)
    if not url.endswith('/'): url += '/'
    parsed_url = urlparse(url)
    user = None
    password = None
    if '@' in parsed_url.netloc:
        user_info, host = parsed_url.netloc.split('@')
        if ':' in user_info:
            user, password = user_info.split(':')
    else: host = parsed_url.netloc
    rpath = parsed_url.path
    dav_url = "%s://%s%s" % (parsed_url.scheme, host, rpath)
    r = requests.request('PROPFIND', dav_url, verify=False, auth=(user, password))
    r.raise_for_status()
    tree = parse(StringIO(r.content))
    if not os.path.isdir(lpath): os.makedirs(lpath)
    for elem in tree.findall('{DAV:}response'):
        collection = elem.find('{DAV:}propstat/{DAV:}prop/{DAV:}resourcetype/{DAV:}collection')
        if collection is not None: continue
        href = elem.find('{DAV:}href').text
        rel_path = os.path.relpath(href, rpath)
        file_url = os.path.join(dav_url, rel_path)
        local_path = os.path.join(lpath, rel_path)
        local_dir = os.path.dirname(local_path)
        if not os.path.exists(local_dir): os.makedirs(local_dir)
        resp = requests.request('GET', file_url, verify=False, stream=True, auth=(user, password))
        resp.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
    return os.path.abspath(lpath)


if __name__ == "__main__":
    url = sys.argv[1]
    get_remote_dav(url)
