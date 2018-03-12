#!/usr/bin/env python
import os, sys, json, re, socket
from glob import glob
from datetime import datetime

from prov_es.model import get_uuid, ProvEsDocument

PLATFORM_RE = re.compile(r'/(CSKS\d)')


def get_netsel_urls(netsel_file):
    """Return list of CSK urls."""

    # use sorted list of urls of the InSAR inputs to create hash
    with open(netsel_file) as f:
        netsel_json = json.load(f)
    urls = []
    for i in netsel_json:
        for j in i: urls.append(j['url'])
    return urls


def create_prov_es_json(id, netsel_file, jobdesc_file, project, aria_dem_xml,
                        aria_dem_file, prod_dir, work_dir, prov_file):
    """Create provenance JSON file."""

    # put in fake start/end times so that prov:used and prov:generated
    # are properly created by the prov lib
    fake_time = datetime.utcnow().isoformat() + 'Z'
    job_id = "create_interferogram-%s" % fake_time
    bundle_id = "bundle-create_interferogram-%s" % fake_time

    # create PROV-ES doc
    doc = ProvEsDocument()
    #bndl = doc.bundle("hysds:%s" % get_uuid(bundle_id))
    bndl = None

    # input and output identifiers
    input_ids = {}
    platform_ids = {}
    instrument_ids = {}

    # full url paths
    work_url = "file://%s%s" % (socket.getfqdn(), work_dir)
    prod_url = "%s/%s" % (work_url, prod_dir)

    # add network selector file
    #netsel_ent = bndl.entity("hysds:%s" % get_uuid("%s/%s" % (work_url, netsel_file)),
    netsel_ent = doc.file("hysds:%s" % get_uuid("%s/%s" % (work_url, netsel_file)),
                          ["%s/%s" % (work_url, netsel_file)],
                          label=os.path.basename(netsel_file))
    input_ids[netsel_ent.identifier] = True
    
    # add job description file
    #jobdesc_ent = bndl.entity("hysds:%s" % get_uuid("%s/%s" % (work_url, jobdesc_file)),
    jobdesc_ent = doc.file("hysds:%s" % get_uuid("%s/%s" % (work_url, jobdesc_file)),
                           ["%s/%s" % (work_url, jobdesc_file)],
                           label=os.path.basename(jobdesc_file))
    input_ids[jobdesc_ent.identifier] = True
    
    # get list of CSK urls
    level = "L0"
    version = "v1.0"
    sensor = "eos:SAR"
    sensor_title = "Synthetic-aperture radar (SAR)"
    gov_org = "eos:ASI"
    gov_org_title = "Agenzia Spaziale Italiana"
    doc.governingOrganization(gov_org, label=gov_org_title, bundle=bndl)
    instrument = ""
    for i, url in enumerate(get_netsel_urls(netsel_file)):
        match = PLATFORM_RE.search(url)
        if not match: continue
        pf = match.group(1)
        platform = "eos:%s" % pf
        platform_title = "COSMO-SkyMed Satellite %s" % pf[-1]
        instrument = "eos:%s-SAR" % pf
        instrument_title = "%s-SAR" % pf
        input_ds = doc.product("hysds:%s" % get_uuid(url), None,
                               [url], [instrument], None, level, version,
                               label=os.path.basename(url), bundle=bndl)
        input_ids[input_ds.identifier] = True
        if platform not in platform_ids:
            doc.platform(platform, [instrument], label=platform_title,
                         bundle=bndl)
            platform_ids[platform] = True
        if instrument not in instrument_ids:
            doc.instrument(instrument, platform, [sensor], [gov_org],
                           label=instrument_title, bundle=bndl)
            doc.sensor(sensor, instrument, label=sensor_title, bundle=bndl)
            instrument_ids[instrument] = True

    # add dem xml, file and related provenance
    srtm_platform = "eos:SpaceShuttleEndeavour"
    srtm_platform_title = "USS Endeavour"
    srtm_instrument = "eos:SRTM"
    srtm_instrument_title = "Shuttle Radar Topography Mission (SRTM)"
    srtm_sensor = "eos:radar"
    srtm_sensor_title = "radar"
    srtm_gov_org = "eos:JPL"
    srtm_gov_org_title = "Jet Propulsion Laboratory"
    doc.governingOrganization(srtm_gov_org, label=srtm_gov_org_title, bundle=bndl)
    #dem_xml_ent = bndl.entity("hysds:%s" % get_uuid("%s/%s" % (work_url, aria_dem_xml)),
    dem_xml_ent = doc.file("hysds:%s" % get_uuid("%s/%s" % (work_url, aria_dem_xml)),
                           ["%s/%s" % (work_url, aria_dem_xml)],
                           label=os.path.basename(aria_dem_xml))
    input_ids[dem_xml_ent.identifier] = True
    #dem_file_ent = bndl.entity("hysds:%s" % get_uuid("%s/%s" % (work_url, aria_dem_file)),
    dem_file_ent = doc.file("hysds:%s" % get_uuid("%s/%s" % (work_url, aria_dem_file)),
                            ["%s/%s" % (work_url, aria_dem_file)],
                            label=os.path.basename(aria_dem_file))
    input_ids[dem_file_ent.identifier] = True
    doc.platform(srtm_platform, [srtm_instrument], label=srtm_platform_title,
                 bundle=bndl)
    doc.instrument(srtm_instrument, srtm_platform, [srtm_sensor], [srtm_gov_org],
                   label=srtm_instrument_title, bundle=bndl)
    doc.sensor(srtm_sensor, srtm_instrument, label=srtm_sensor_title, bundle=bndl)
    instrument_ids[srtm_instrument] = True

    # software and algorithm
    algorithm = "eos:interferogram_generation"
    software_version = "2.0.0_201604"
    software_title = "InSAR SCE (InSAR Scientific Computing Environment) v%s" % software_version
    software = "eos:ISCE-%s" % software_version
    software_location = "https://winsar.unavco.org/isce.html"
    doc.software(software, [algorithm], software_version, label=software_title,
                 location=software_location, bundle=bndl)

    # output
    int_level = "L2"
    int_version = "v1.0"
    int_collection = "eos:CSK-interferograms-%s" % int_version
    int_collection_shortname = "CSK-interferograms-%s" % int_version
    int_collection_label = "ISCE generated CSK interferograms %s" % int_version
    int_collection_loc = "https://aria-dav.jpl.nasa.gov/repository/products/interferogram/%s" % int_version
    doc.collection(int_collection, None, int_collection_shortname,
                   int_collection_label, [int_collection_loc],
                   instrument_ids.keys(), int_level, int_version,
                   label=int_collection_label, bundle=bndl)
    output_ds = doc.granule("hysds:%s" % get_uuid(prod_url), None, [prod_url], 
                            instrument_ids.keys(), int_collection, int_level,
                            int_version, label=id, bundle=bndl)

    # runtime context
    rt_ctx_id = "hysds:runtimeContext-ariamh-%s" % project
    doc.runtimeContext(rt_ctx_id, [project], label=project, bundle=bndl)

    # create process
    doc.processStep("hysds:%s" % get_uuid(job_id), fake_time, fake_time,
                    [software], None, rt_ctx_id, input_ids.keys(), 
                    [output_ds.identifier], label=job_id, bundle=bndl,
                    prov_type="hysds:create_interferogram")
     
    # write
    with open(prov_file, 'w') as f:
        json.dump(json.loads(doc.serialize()), f, indent=2, sort_keys=True)


if __name__ == "__main__":
    if len(sys.argv) != 10:
        print("%s <id> <network selector> <job description> <project> <DEM xml URL> <DEM file URL> <product dir> <work dir> <PROV-ES JSON file>" % sys.argv[0])
        sys.exit(1)

    create_prov_es_json(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9])
    sys.exit(0)
