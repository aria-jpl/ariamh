#!/usr/bin/env python
import os, sys, json, urllib2, re
from lxml import etree
from urllib import urlencode

VERSION_RE = re.compile(r'CSKS\d_.*?_(?P<calibrated>[BU])_.*?_(?P<orbits>[SF]F)_.*?\.h5')

BEAM_MAP = {
    '01': [22.6, 25.66],
    '02': [23.13, 26.21],
    '03': [25.1, 28.0],
    '04': [27.71, 30.47],
    '05': [29.27, 31.96],
    '06': [30.6, 33.38],
    '07': [32.43, 34.83],
    '08': [33.6, 36.0],
    '09': [34.6, 37.2],
    '0A': [16.36, 20.15],
    '0B': [20.05, 23.5],
    '10': [35.9, 38.15],
    '11': [37.51, 39.6],
    '12': [38.56, 40.67],
    '13': [39.34, 41.39],
    '14': [40.0, 42.0],
    '15': [41.79, 43.62],
    '16': [43.1, 44.8],
    '17': [44.49, 45.92],
    '18': [45.69, 46.85],
    '19': [46.8, 47.99],
    '20': [47.69, 48.7],
    '21': [48.64, 49.8],
    '22': [49.66, 50.64],
    '23': [50.52, 51.37],
    '24': [51.15, 51.98]
}


def get_dfdn(dfdn_file):
    """Return DFAS metadata."""

    et = etree.parse(dfdn_file)
    dfdn = {}
    dfdn['ProductName'] = et.xpath('.//ProductName/text()')[0]
    dfdn['ProductId'] = int(et.xpath('.//ProductId/text()')[0])
    dfdn['ProductGenerationDate'] = et.xpath('.//ProductGenerationDate/text()')[0]
    dfdn['UserRequestId'] = et.xpath('.//UserRequestId/text()')[0]
    dfdn['ServiceRequestName'] = et.xpath('.//ServiceRequestName/text()')[0]
    dfdn['ProductType'] = et.xpath('.//ProductType/text()')[0]
    dfdn['SceneSensingStartUTC'] = et.xpath('.//SceneSensingStartUTC/text()')[0]
    dfdn['SceneSensingStopUTC'] = et.xpath('.//SceneSensingStopUTC/text()')[0]
    dfdn['SatelliteId'] = et.xpath('.//SatelliteId/text()')[0]
    dfdn['AcquisitionMode'] = et.xpath('.//AcquisitionMode/text()')[0]
    dfdn['FormatType'] = et.xpath('.//FormatType/text()')[0]
    dfdn['ProdSpecDocument'] = et.xpath('.//ProdSpecDocument/text()')[0]
    dfdn['ProcessingCentre'] = et.xpath('.//ProcessingCentre/text()')[0]
    dfdn['ProviderId'] = et.xpath('.//ProviderId/text()')[0]

    # additional metadata for CSK KML docs
    dfdn['GeoCoordBottomRight'] = map(float, et.xpath('.//GeoCoordBottomRight/text()')[0].split())
    dfdn['GeoCoordBottomLeft'] = map(float, et.xpath('.//GeoCoordBottomLeft/text()')[0].split())
    dfdn['GeoCoordTopRight'] = map(float, et.xpath('.//GeoCoordTopRight/text()')[0].split())
    dfdn['GeoCoordTopLeft'] = map(float, et.xpath('.//GeoCoordTopLeft/text()')[0].split())
    dfdn['GeoCoordSceneCentre'] = map(float, et.xpath('.//GeoCoordSceneCentre/text()')[0].split())
    dfdn['LookSide'] = et.xpath('.//LookSide/text()')[0]

    if dfdn['GeoCoordTopRight'][0] > dfdn['GeoCoordBottomRight'][0]:
        dfdn['Geometry'] = 'DESCENDING'
    else:
        dfdn['Geometry'] = 'ASCENDING'

    dfdn['SatelliteNumber'] = int(dfdn['ProductName'][4])
    if 'RAW' in dfdn['ProductName']:
        dfdn['Format'] = 'RAW'
    else:
        dfdn['Format'] = 'SLC'

    return dfdn


def get_center(dfdn_file):
    """Return (lon, lat) values for the center."""

    et = etree.parse(dfdn_file)
    vals = et.xpath('.//GeoCoordSceneCentre')[0].text.split()
    return float(vals[1]), float(vals[0])


def add_dfdn_metadata(metadata_file, dfdn_file):
    """Add DFDN metadata to metadata json file."""

    # add center location
    lon, lat = get_center(dfdn_file)
    with open(metadata_file) as f:
        metadata = json.load(f)
    metadata['center'] = {
        'type':        'point',
        'coordinates': [ lon, lat ]
    }

    # add dfdn metadata
    dfdn = get_dfdn(dfdn_file)
    metadata['dfdn'] = dfdn

    # add beam number
    metadata['beamNumber'] = os.path.basename(dfdn_file)[20:22]

    # add version
    dfdn_basename = os.path.basename(dfdn_file)
    match = VERSION_RE.search(dfdn_basename)
    if not match:
        raise RuntimeError("Failed to extract calibrate and orbit fields from name: %s" % dfdn_basename)
    version = "%s-%s" % match.groups()
    metadata['version'] = version

    # overwrite metadata json file
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("%s <metadata JSON file> <DFDN file>" % sys.argv[0])
        sys.exit(1)

    add_dfdn_metadata(sys.argv[1], sys.argv[2])
    sys.exit(0)
