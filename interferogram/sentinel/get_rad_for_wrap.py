#! /usr/bin/env python3
import os, sys, json, httplib2, hashlib, time, re
from urllib.parse import urlparse
from lxml.etree import parse
import numpy as np


def main():
    """Extract radian value for 5-cm wrap from master.xml."""

    input_file = sys.argv[1]
    rt = parse(input_file)
    wv = eval(rt.xpath('.//property[@name="radarwavelength"]/value/text()')[0])
    rad = 4 * np.pi * .05 / wv
    print(rad)


if __name__ == "__main__":
    main()
