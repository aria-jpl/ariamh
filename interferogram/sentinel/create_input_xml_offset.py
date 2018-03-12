#! /usr/bin/env python3
import os, sys
from string import Template


def create_input_xml_offset(tmpl_file, xml_file, master_safe_dir, slave_safe_dir,
                     master_orbit, slave_orbit, dem_file, swathnum, ampcor_skip_width,
                     ampcor_skip_height,ampcor_src_win_width,ampcor_src_win_height,
                     ampcor_src_width,ampcor_src_height):
    with open(tmpl_file) as f:
        tmpl = Template(f.read())
    with open(xml_file, 'w') as f:
        f.write(tmpl.safe_substitute(MASTER_SAFE_DIR=master_safe_dir,
                                     SLAVE_SAFE_DIR=slave_safe_dir,
                                     MASTER_ORBIT_FILE=master_orbit,
                                     SLAVE_ORBIT_FILE=slave_orbit,
                                     DEM_FILE=dem_file,
                                     SWATHNUM=swathnum,
                                     AMPCOR_SKIP_WIDTH=ampcor_skip_width,
                                     AMPCOR_SKIP_HEIGHT=ampcor_skip_height,
                                     AMPCOR_SRC_WIN_WIDTH=ampcor_src_win_width,
                                     AMPCOR_SRC_WIN_HEIGHT=ampcor_src_win_height,
                                     AMPCOR_SRC_WIDTH=ampcor_src_width,
                                     AMPCOR_SRC_HEIGHT=ampcor_src_height
                                    ))                              

def main():
    """Create sentinel.ini."""

    tmpl_file = sys.argv[1]
    xml_file = sys.argv[2]
    master_safe_dir = sys.argv[3]
    slave_safe_dir = sys.argv[4]
    master_orbit = sys.argv[5]
    slave_orbit = sys.argv[6]
    dem_file = sys.argv[7]
    swathnum = sys.argv[8]
    ampcor_skip_width = sys.argv[9]
    ampcor_skip_height = sys.argv[10]
    ampcor_src_win_width = sys.argv[11]
    ampcor_src_win_height = sys.argv[12]
    ampcor_src_width = sys.argv[13]
    ampcor_src_height = sys.argv[14]
    
    create_input_xml_offset(tmpl_file, xml_file, master_safe_dir, slave_safe_dir,
                     master_orbit, slave_orbit, dem_file, swathnum, ampcor_skip_width,
                     ampcor_skip_height,ampcor_src_win_width,ampcor_src_win_height,
                     ampcor_src_width,ampcor_src_height)

    # get metadata
    if not os.path.exists(xml_file):
        raise RuntimeError("Failed to find $s." % xml_file)


if __name__ == "__main__":
    main()

