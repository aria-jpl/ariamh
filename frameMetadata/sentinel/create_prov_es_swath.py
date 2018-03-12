#!/usr/bin/env python
import os, sys, json, socket, re
from glob import glob
from datetime import datetime

from prov_es.model import get_uuid, ProvEsDocument


S1_RE = re.compile(r'^(s1\w)-iw(\d)-(\w{3})-.*$')
MISSION_RE = re.compile(r'^S1(\w)_')


def create_prov_es_json(ctx_file, id, prod_dir, prov_file):
    """Create provenance JSON file."""

    # get abs path
    prod_dir = os.path.abspath(prod_dir)

    # get context
    with open(ctx_file) as f:
        context = json.load(f)

    # get mission char
    mis_char = MISSION_RE.search(context.get('file')).group(1)
    mis_char_lc = mis_char.lower()

    # get input url
    input_url = context.get('localize_urls', [{ 'url': None}])[0]['url']

    # get info
    s1_files = glob(os.path.join(prod_dir, "s1%s-*.tiff" % mis_char_lc))
    pf = "S1%s" % mis_char
    dtype = "NA" 
    repo_dir = "?"
    if len(s1_files) > 0:
        match = S1_RE.search(os.path.basename(s1_files[0]))
        if match: pf, swathnum, dtype = match.groups()
        if dtype == "raw":
            dtype = "RAW"
            repo_dir = "s1%s_raw" % mis_char_lc
        elif dtype == "slc":
            dtype = "SLC"
            repo_dir = "s1%s_slc" % mis_char_lc
    platform = "eos:%s" % pf
    platform_title = "Sentinel1%s Satellite" % mis_char
    instrument = "eos:%s-SAR" % pf
    instrument_title = "%s-SAR" % pf
    level = "L0"
    version = "v1.0"
    collection = "eos:S1%s-%s-%s" % (mis_char, dtype, version)
    collection_shortname = "S1%s-%s-%s" % (mis_char, dtype, version)
    collection_label = "S1%s %s Scenes %s" % (mis_char, dtype, version)
    collection_loc = "https://aria-dst-dav.jpl.nasa.gov/repository/products/%s/%s" % (repo_dir, version)
    sensor = "eos:SAR"
    sensor_title = "Synthetic-aperture radar (SAR)"
    gov_org = "eos:ESA"
    gov_org_title = "European Space Agency"
    software_version = "2.0.0_201604"
    software_title = "InSAR SCE (InSAR Scientific Computing Environment) v%s" % software_version
    software = "eos:ISCE-%s" % software_version
    software_location = "https://winsar.unavco.org/isce.html"
    algorithm = "eos:metadata_extraction"
    prod_dir = "file://%s%s" % (socket.getfqdn(), prod_dir)
    
    # put in fake start/end times so that prov:used and prov:generated
    # are properly created by the prov lib
    fake_time = datetime.utcnow().isoformat() + 'Z'
    job_id = "ingest-%s-%s" % (id, fake_time)
    bundle_id = "bundle-ingest-%s-%s" % (id, fake_time)

    doc = ProvEsDocument()
    #bndl = doc.bundle("hysds:%s" % get_uuid(bundle_id))
    bndl = None
    input_id = "hysds:%s" % get_uuid(input_url)
    input_ds = doc.granule(input_id, None, [input_url], [instrument], None, level,
                           None, label=os.path.basename(input_url), bundle=bndl)
    doc.collection(collection, None, collection_shortname, collection_label,
                   [collection_loc], [instrument], level, version, 
                   label=collection_label, bundle=bndl)
    output_id = "hysds:%s" % get_uuid(prod_dir)
    output_ds = doc.granule(output_id, None, [prod_dir], [instrument],
                            collection, level, version, label=id,
                            bundle=bndl)
    doc.governingOrganization(gov_org, label=gov_org_title, bundle=bndl)
    doc.platform(platform, [instrument], label=platform_title, bundle=bndl)
    doc.instrument(instrument, platform, [sensor], [gov_org],
                   label=instrument_title, bundle=bndl)
    doc.sensor(sensor, instrument, label=sensor_title, bundle=bndl)
    doc.software(software, [algorithm], software_version, label=software_title,
                 location=software_location, bundle=bndl)
    doc.processStep("hysds:%s" % get_uuid(job_id), fake_time, fake_time, 
                    [software], None, None, [input_ds.identifier],
                    [output_ds.identifier], label=job_id, bundle=bndl,
                    prov_type="hysds:ingest")
     
    with open(prov_file, 'w') as f:
        json.dump(json.loads(doc.serialize()), f, indent=2, sort_keys=True)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("%s <context JSON> <product id> <product dir> <PROV-ES JSON file>" % sys.argv[0])
        sys.exit(1)

    create_prov_es_json(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    sys.exit(0)
