#! /usr/bin/env python3
import os, sys
from string import Template


def create_input_xml(tmpl_file, xml_file, reference_data_dir, secondary_data_dir,
                     dem_file, geocoded_dem_file,  start_swathnum, end_swathnum, burst_overlap,
                     ref_pol, ref_frame_arr, sec_pol, sec_frame_arr, bbox):
    with open(tmpl_file) as f:
        tmpl = Template(f.read())
    with open(xml_file, 'w') as f:
        f.write(tmpl.safe_substitute(REFERENCE_DATA_DIR=reference_data_dir,
                                     SECONDARY_DATA_DIR=secondary_data_dir,
                                     DEM_FILE=dem_file,
                                     GEOCODED_DEM_FILE=geocoded_dem_file,
                                     START_SUBSWATH=start_swathnum,
                                     END_SUBSWATH=end_swathnum,
                                     BURST_OVERLAP=burst_overlap,
                                     REF_POL=ref_pol,
                                     REF_FRAME=ref_frame_arr,
                                     SEC_POL=sec_pol,
                                     SEC_FRAME=sec_frame_arr,
                                     BBOX=bbox))
                                     

def main():
    """Create sentinel.ini."""

    tmpl_file = sys.argv[1]
    xml_file = sys.argv[2]
    reference_data_dir = sys.argv[3]
    secondary_data_dir = sys.argv[4]
    dem_file = sys.argv[5]
    geocoded_dem_file = sys.argv[6]
    start_swathnum = sys.argv[7]
    end_swathnum = sys.argv[8]
    burst_overlap = sys.argv[9]
    ref_pol = sys.argv[10]
    ref_frame_arr = sys.argv[11]
    sec_pol = sys.argv[12]
    sec_frame_arr = sys.argv[13]
    bbox = sys.argv[14].split(' ')
    print(bbox)
    print(tmpl_file)
    '''
    azimuth_looks = sys.argv[11]
    range_looks = sys.argv[12]
    filter_strength = sys.argv[13]
    bbox = sys.argv[14]
    use_virtual_files = sys.argv[15]
    do_esd = sys.argv[16]
    esd_coherence_threshold = sys.argv[17]
    '''
    create_input_xml(tmpl_file, xml_file, reference_data_dir, secondary_data_dir,
                     dem_file, geocoded_dem_file,  start_swathnum, end_swathnum, burst_overlap,
                     ref_pol, ref_frame_arr, sec_pol, sec_frame_arr, bbox)

    # get metadata
    if not os.path.exists(xml_file):
        raise RuntimeError("Failed to find $s." % xml_file)


if __name__ == "__main__":
    main()
