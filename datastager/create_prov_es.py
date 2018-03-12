#!/usr/bin/env python
import os, sys, json, socket, re
from glob import glob
from datetime import datetime

from prov_es.model import get_uuid, ProvEsDocument


CSK_RE = re.compile(r'^(CSKS\d)_(\w{3})_.*$')


def create_prov_es_json(id, url, prod_dir, prov_file):
    """Create provenance JSON file."""

    # get info
    csk_files = glob(os.path.join(prod_dir, "CSKS*"))
    pf = "CSKS?"
    dtype = "NA" 
    repo_dir = "?"
    if len(csk_files) > 0:
        match = CSK_RE.search(os.path.basename(csk_files[0]))
        if match: pf, dtype = match.groups()
        if dtype == "RAW":
            dtype = "RAW_B"
            repo_dir = "csk_rawb"
        elif dtype == "SCS":
            dtype = "SCS_B"
            repo_dir = "csk_scsb"
    platform = "eos:%s" % pf
    platform_title = "COSMO-SkyMed Satellite %s" % pf[-1]
    instrument = "eos:%s-SAR" % pf
    instrument_title = "%s-SAR" % pf
    level = "L0"
    version = "v1.0"
    collection = "eos:CSK-%s-%s" % (dtype, version)
    collection_shortname = "CSK-%s-%s" % (dtype, version)
    collection_label = "CSK %s Scenes %s" % (dtype, version)
    collection_loc = "https://aria-dav.jpl.nasa.gov/repository/products/%s/%s" % (repo_dir, version)
    sensor = "eos:SAR"
    sensor_title = "Synthetic-aperture radar (SAR)"
    gov_org = "eos:ASI"
    gov_org_title = "Agenzia Spaziale Italiana"
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
    input_id = "hysds:%s" % get_uuid(url)
    input_ds = doc.granule(input_id, None, [url], [instrument], None, level,
                           None, label=os.path.basename(url), bundle=bndl)
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
        print("%s <product id> <input URL> <product dir> <PROV-ES JSON file>" % sys.argv[0])
        sys.exit(1)

    create_prov_es_json(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    sys.exit(0)
