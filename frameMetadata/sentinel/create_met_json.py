#!/usr/bin/env python
import os, sys, json, re
from lxml.etree import parse


def create_met_json(xml_file, json_file, mis_char):
    """Write product metadata json."""

    with open(xml_file) as f:
        doc = parse(f)
    coords = doc.xpath("//*[local-name() = 'coordinates']")[0].text        
    bbox = []
    for coord in coords.split():
        lat, lon = coord.split(',')
        bbox.append(map(float, [lat, lon]))
    ipf_version = doc.xpath("//*[local-name() = 'software']/@version")[0]
    sensing_start = doc.xpath("//*[local-name() = 'startTime']")[0].text
    sensing_stop = doc.xpath("//*[local-name() = 'stopTime']")[0].text
    downlink_start = doc.xpath("//*[local-name() = 'resource'][@name = 'Downlinked Stream']/*[local-name() = 'processing']/@start")[0]
    downlink_stop = doc.xpath("//*[local-name() = 'resource'][@name = 'Downlinked Stream']/*[local-name() = 'processing']/@stop")[0]
    l0_start = doc.xpath("//*[local-name() = 'resource'][@role = 'Raw Data']/*[local-name() = 'processing']/@start")[0]
    l0_stop = doc.xpath("//*[local-name() = 'resource'][@role = 'Raw Data']/*[local-name() = 'processing']/@stop")[0]
    processing_start = doc.xpath("//*[local-name() = 'processing'][@name = 'SLC Processing']/@start")[0]
    processing_stop = doc.xpath("//*[local-name() = 'processing'][@name = 'SLC Processing']/@stop")[0]
    post_processing_start = doc.xpath("//*[local-name() = 'processing'][@name = 'SLC Post Processing']/@start")[0]
    post_processing_stop = doc.xpath("//*[local-name() = 'processing'][@name = 'SLC Post Processing']/@stop")[0]
    orbit = int(doc.xpath("//*[local-name() = 'orbitNumber']")[0].text)
    track = int(doc.xpath("//*[local-name() = 'relativeOrbitNumber']")[0].text)
    cycle = int(doc.xpath("//*[local-name() = 'cycleNumber']")[0].text)
    direction = doc.xpath("//*[local-name() = 'pass']")[0].text
    if direction == "ASCENDING": direction = "asc"
    else: direction = "dsc"
    archive_filename = os.path.basename(json_file).replace('.met.json', '.zip')
    metadata = {
        "archive_filename": archive_filename,
        "bbox": bbox,
        "platform": "Sentinel-1%s" % mis_char,
        "sensor": "SAR-C Sentinel1",
        "sensingStart": sensing_start.replace('Z', ''),
        "sensingStop": sensing_stop.replace('Z', ''),
        "downlinkStart": downlink_start.replace('Z', ''),
        "downlinkStop": downlink_stop.replace('Z', ''),
        "l0ProcessingStart": l0_start.replace('Z', ''),
        "l0ProcessingStop": l0_stop.replace('Z', ''),
        "processingStart": processing_start.replace('Z', ''),
        "processingStop": processing_stop.replace('Z', ''),
        "postProcessingStart": post_processing_start.replace('Z', ''),
        "postProcessingStop": post_processing_stop.replace('Z', ''),
        "trackNumber": track,
        "orbitNumber": orbit,
        "orbitRepeat": 175,
        "orbitCycle": cycle,
        "direction": direction,
        "version": ipf_version,
    }
    with open(json_file, 'w') as f:
        json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("usage: %s <manifest.safe> <output json file> <mis_char>" % sys.argv[0])
    xml_file = sys.argv[1]
    json_file = sys.argv[2]
    mis_char = sys.argv[3]
    create_met_json(xml_file, json_file, mis_char)
