#! /usr/bin/env python3
import os, sys
from string import Template


def create_input_xml(tmpl_file, xml_file, master_safe_dir, slave_safe_dir,
                     master_orbit, slave_orbit, master_pol, slave_pol,
                     dem_file, geocode_dem_file, swathnum, azimuth_looks, range_looks,
                     filter_strength, bbox, use_virtual_files, do_esd, 
                     esd_coherence_threshold):
    with open(tmpl_file) as f:
        tmpl = Template(f.read())
    with open(xml_file, 'w') as f:
        f.write(tmpl.safe_substitute(MASTER_SAFE_DIR=master_safe_dir,
                                     SLAVE_SAFE_DIR=slave_safe_dir,
                                     MASTER_ORBIT_FILE=master_orbit,
                                     SLAVE_ORBIT_FILE=slave_orbit,
                                     MASTER_POL=master_pol,
                                     SLAVE_POL=slave_pol,
                                     DEM_FILE=dem_file,
                                     GEOCODE_DEM_FILE=geocode_dem_file,
                                     SWATHNUM=swathnum,
                                     AZIMUTH_LOOKS=azimuth_looks,
                                     RANGE_LOOKS=range_looks,
                                     FILTER_STRENGTH=filter_strength,
                                     BBOX=[eval(i) for i in bbox.split()],
                                     USE_VIRTUAL_FILES=use_virtual_files,
                                     DO_ESD=do_esd,
                                     ESD_COHERENCE_THRESHOLD=esd_coherence_threshold))
                                     

def main():
    """Create sentinel.ini."""

    tmpl_file = sys.argv[1]
    xml_file = sys.argv[2]
    master_safe_dir = sys.argv[3]
    slave_safe_dir = sys.argv[4]
    master_orbit = sys.argv[5]
    slave_orbit = sys.argv[6]
    master_pol = sys.argv[7]
    slave_pol = sys.argv[8]
    dem_file = sys.argv[9]
    geocode_dem_file = sys.argv[10]
    swathnum = sys.argv[11]
    azimuth_looks = sys.argv[12]
    range_looks = sys.argv[13]
    filter_strength = sys.argv[14]
    bbox = sys.argv[15]
    use_virtual_files = sys.argv[16]
    do_esd = sys.argv[17]
    esd_coherence_threshold = sys.argv[18]
    create_input_xml(tmpl_file, xml_file, master_safe_dir, slave_safe_dir,
                     master_orbit, slave_orbit, master_pol, slave_pol,
                     dem_file, geocode_dem_file, swathnum, azimuth_looks, range_looks, 
                     filter_strength, bbox, use_virtual_files, do_esd, 
                     esd_coherence_threshold)

    # get metadata
    if not os.path.exists(xml_file):
        raise RuntimeError("Failed to find $s." % xml_file)


if __name__ == "__main__":
    main()
