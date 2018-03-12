#!/usr/bin/env python
import os, sys, json, re, socket
from glob import glob
from datetime import datetime

from prov_es.model import get_uuid, ProvEsDocument

PLATFORM_RE = re.compile(r'/S1A_IW_(\w+?)_')


def create_prov_es_json(id, project, master_orbit_file, slave_orbit_file,
                        aria_dem_xml, aria_dem_file, work_dir, prov_file):
    """Create provenance JSON file."""

    # get abs paths
    work_dir = os.path.abspath(work_dir)
    prod_dir = os.path.join(work_dir, id)

    # get context
    ctx_file = os.path.join(prod_dir, "%s.context.json" % id)
    with open(ctx_file) as f:
        context = json.load(f)

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
    prod_url = "%s/%s" % (work_url, id)

    # add sentinel.ini file
    ini_ent = doc.file("hysds:%s" % get_uuid("%s/sentinel.ini" % work_url),
                          ["%s/sentinel.ini" % work_url],
                          label="sentinel.ini")
    input_ids[ini_ent.identifier] = True
    
    # add orbit files
    master_orbit_ent = doc.file("hysds:%s" % get_uuid("%s/%s" % (work_url, master_orbit_file)),
                                ["%s/%s" % (work_url, master_orbit_file)],
                                label=os.path.basename(master_orbit_file))
    input_ids[master_orbit_ent.identifier] = True
    slave_orbit_ent = doc.file("hysds:%s" % get_uuid("%s/%s" % (work_url, slave_orbit_file)),
                                ["%s/%s" % (work_url, slave_orbit_file)],
                                label=os.path.basename(slave_orbit_file))
    input_ids[slave_orbit_ent.identifier] = True
    
    # get list of S1A urls
    level = "L0"
    version = "v1.0"
    sensor = "eos:SAR"
    sensor_title = "Synthetic-aperture radar (SAR)"
    gov_org = "eos:ESA"
    gov_org_title = "European Space Agency"
    doc.governingOrganization(gov_org, label=gov_org_title, bundle=bndl)
    instrument = ""
    for i, url in enumerate([ context.get('master_zip_url', ''), context.get('slave_zip_url', '') ]):
        match = PLATFORM_RE.search(url)
        if not match: continue
        pf = match.group(1)
        platform = "eos:%s" % pf
        platform_title = "Sentinel1A Satellite"
        instrument = "eos:%s-SAR" % pf
        instrument_title = "%s-SAR" % pf
        input_ds = doc.product("hysds:%s" % get_uuid(url), None,
                               [url], [instrument], None, level, None,
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
    dem_xml_ent = doc.file("hysds:%s" % get_uuid("%s/%s" % (work_url, aria_dem_xml)),
                           ["%s/%s" % (work_url, aria_dem_xml)],
                           label=os.path.basename(aria_dem_xml))
    input_ids[dem_xml_ent.identifier] = True
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
    int_collection = "eos:S1A-interferograms-%s" % int_version
    int_collection_shortname = "S1A-interferograms-%s" % int_version
    int_collection_label = "ISCE generated S1A interferograms %s" % int_version
    int_collection_loc = "https://aria-dst-dav.jpl.nasa.gov/products/s1a_ifg/%s" % int_version
    doc.collection(int_collection, None, int_collection_shortname,
                   int_collection_label, [int_collection_loc],
                   instrument_ids.keys(), int_level, int_version,
                   label=int_collection_label, bundle=bndl)
    output_ds = doc.granule("hysds:%s" % get_uuid(prod_url), None, [prod_url], 
                            instrument_ids.keys(), int_collection, int_level,
                            int_version, label=id, bundle=bndl)

    # runtime context
    rt_ctx_id = "hysds:runtimeContext-sentinel_ifg-%s" % project
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
    if len(sys.argv) != 9:
        print("%s <id> <project> <master orbit file> <slave orbit file> <DEM xml> <DEM file> <work dir> <PROV-ES JSON file>" % sys.argv[0])
        sys.exit(1)

    create_prov_es_json(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8])
    sys.exit(0)
