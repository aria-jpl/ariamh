#! /usr/bin/env python3
import os, sys, json, httplib2, hashlib, time, re
from urllib.parse import urlparse
from lxml.etree import parse


def main():
    """Extract size from unw.geo.xml."""

    input_file = sys.argv[1]
    rt = parse(input_file)
    size = rt.xpath('.//component[@name="coordinate1"]/property[@name="size"]/value/text()')[0]
    print(size)


if __name__ == "__main__":
    main()
