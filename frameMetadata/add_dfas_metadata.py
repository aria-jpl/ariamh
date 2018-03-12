#!/usr/bin/env python
import re, sys, json, urllib2
from lxml import etree


XSL_RE = re.compile(r'^<\?xml-stylesheet.*?\?>\n')


def get_dfas(dfas_file):
    """Return DFAS metadata."""

    with open(dfas_file) as f:
        xml = f.read()
    xml = XSL_RE.sub('', xml)
    et = etree.XML(xml)
    dfas = {}
    dfas['RequestorUserId'] = et.xpath('.//RequestorUserId/text()')[0]
    dfas['RequestId'] = int(et.xpath('.//RequestId/text()')[0])
    dfas['ServiceRequestName'] = et.xpath('.//ServiceRequestName/text()')[0]
    dfas['UserRequestName'] = et.xpath('.//UserRequestName/text()')[0]
    dfas['Note'] = et.xpath('.//Note/text()')[0]
    dfas['DeliveryRequestId'] = int(et.xpath('.//DeliveryRequestId/text()')[0])
    dfas['DeliveryDateUTC'] = et.xpath('.//DeliveryDateUTC/text()')[0]
    dfas['ProductId'] = int(et.xpath('.//ProductId/text()')[0])
    dfas['ProductType'] = et.xpath('.//ProductType/text()')[0]
    dfas['ProductFormat'] = et.xpath('.//ProductFormat/text()')[0]
    dfas['FormatVersion'] = et.xpath('.//FormatVersion/text()')[0]
    dfas['ClassificationLevel'] = et.xpath('.//ClassificationLevel/text()')[0]
    dfas['ProductFileName'] = et.xpath('.//ProductFileName/text()')[0]
    dfas['ProductSpecificationDocument'] = et.xpath('.//ProductSpecificationDocument/text()')[0]
    dfas['ProcessingCentre'] = et.xpath('.//ProcessingCentre/text()')[0]
    dfas['ProviderId'] = et.xpath('.//ProviderId/text()')[0]
    dfas['CreationDate'] = et.xpath('.//CreationDate/text()')[0]
    dfas['Label'] = et.xpath('.//Label/text()')[0]
    return dfas


def add_dfas_metadata(metadata_file, dfas_file):
    """Add info from DFAS_AccompanyingSheet.xml to metadata."""

    # add dfas metadata
    dfas = get_dfas(dfas_file)
    with open(metadata_file) as f:
        metadata = json.load(f)
    metadata['dfas'] = dfas

    # overwrite metadata json file
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("%s <metadata JSON file> <DFAS file>" % sys.argv[0])
        sys.exit(1)

    add_dfas_metadata(sys.argv[1], sys.argv[2])
    sys.exit(0)
