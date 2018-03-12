#!/usr/bin/env python
import os, sys, json, zipfile
from lxml.etree import parse, tostring


ns = { '_': "http://www.opengis.net/kml/2.2" }


def create_kmz(kml_file, png_file, kmz_file):
    """Create KMZ."""

    kml_file = os.path.abspath(kml_file)
    with open(kml_file) as f:
        doc = parse(f)
    href =  doc.xpath('.//_:href/text()', namespaces=ns)[0]
    doc.xpath('.//_:href', namespaces=ns)[0].text = os.path.basename(href)
    kmz_base, kmz_ext = os.path.splitext(os.path.basename(kmz_file))
    kmz_kml_file = "%s.kml" % kmz_base
    with open(kmz_kml_file, 'w') as f:
        f.write(tostring(doc))
    with zipfile.ZipFile(kmz_file, 'w') as z:
        z.write(kmz_kml_file)
        z.write(png_file)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("usage: %s <kml file> <png file> <kmz file>" % sys.argv[0])
    kml_file = sys.argv[1]
    png_file = sys.argv[2]
    kmz_file = sys.argv[3]
    create_kmz(kml_file, png_file, kmz_file)
