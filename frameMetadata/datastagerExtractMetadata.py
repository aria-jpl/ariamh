#! /usr/bin/env python3
import os, sys, json, httplib2, hashlib, time, re
from urllib.parse import urlparse
from lxml.etree import parse
from string import Template
from pprint import pprint

from extractMetadata import ExtractMetadata


def main():
    """Call metadata extraction making sure only 1 frame is a reference for a region."""

    # get dataset recognizer json
    ds_json_file = os.path.join(os.environ['ARIAMH_HOME'], 'conf', 'recognize_product.json')
    with open(ds_json_file) as f:
        ds_info = json.load(f)

    # extractor input xml file
    input_file = sys.argv[1]
    rt = parse(input_file)
    md_file = rt.xpath('.//property[@name="metadata file"]/value/text()')[0]

    # recognize product and get objectid
    objectid = None
    ipath = None
    for ds in ds_info['datasets']:
        match_re = re.compile(ds['metadata_file_pattern'])
        match = match_re.search(md_file)
        if match:
            objectid = Template(ds['id_template']).substitute(match.groupdict())
            ipath = ds['ipath']
            break
    if objectid is None:
        raise RuntimeError("Couldn't find matching dataset in %s." % ds_json_file)

    # run extractor
    extractor = ExtractMetadata()
    extractor.configure()
    print(extractor.run())

    # get metadata
    if not os.path.exists(md_file):
        raise RuntimeError("Failed to find metdata file %s." % md_file)


if __name__ == "__main__":
    main()
