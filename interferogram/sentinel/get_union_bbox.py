#!/usr/bin/env python3
import os, sys, json, argparse
import numpy as np
from osgeo import ogr, osr

from Sentinel1_TOPS import Sentinel1_TOPS
from FrameInfoExtractor import FrameInfoExtractor as FIE
from extractMetadata_s1 import objectify, S1toFrame


def parse_args():
    """Command line parsing."""

    parser = argparse.ArgumentParser(description='Extract metadata from S1 swath')
    parser.add_argument('-o', '--output', dest='outjson', type=str, required=True,
            help = 'Ouput bbox.json')
    parser.add_argument('xml_file', type=str, nargs='+', help='Swath XML file')
    return parser.parse_args()


def get_loc(xml_file):
    """Return GeoJSON bbox."""

    # read in metadata
    sar = Sentinel1_TOPS()
    sar.xml = xml_file
    sar.parse()
    obj = objectify(xml_file)
    
    # copy into ISCE frame
    frame = S1toFrame(sar,obj)

    # extract frame info
    fie = FIE()
    frameInfo = fie.extractInfoFromFrame(frame.frame)
    bbox = np.array(frameInfo.getBBox()).astype(np.float)
    coords = [
        [ bbox[0,1], bbox[0,0] ],
        [ bbox[1,1], bbox[1,0] ],
        [ bbox[3,1], bbox[3,0] ],
        [ bbox[2,1], bbox[2,0] ],
        [ bbox[0,1], bbox[0,0] ],
    ]
    return {
        "type": "Polygon",
        "coordinates": [ coords ]
    }


def get_union_geom(xml_files):
    geom_union = None
    for xml_file in xml_files:
        loc = get_loc(xml_file)
        geom = ogr.CreateGeometryFromJson(json.dumps(loc))
        if geom_union is None:
            geom_union = geom
        else:
            geom_union = geom_union.Union(geom)
    return geom_union


def get_bbox(xml_files):
    return json.loads(get_union_geom(xml_files).ExportToJson())


def get_envelope(xml_files):
    env = get_union_geom(xml_files).GetEnvelope()
    # reorder for topsApp/ISCE
    return (env[2], env[3], env[0], env[1])


def main():
    args = parse_args()
    #print(json.dumps(get_bbox(args.xml_file), indent=2))
    j = { "envelope": get_envelope(args.xml_file) }
    with open(args.outjson, 'w') as f:
        json.dump(j, f, indent=2)


if __name__ == '__main__':
    main()
