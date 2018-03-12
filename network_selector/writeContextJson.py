#!/usr/bin/env python
import os, sys, json
from glob import glob


def writeContextJson(id, output_file_base, jd_file_base, ns_exit_code, json_file, result_file):
    """Write context json for network selector results."""

    files = glob('%s_*' % output_file_base)
    if len(files) == 0:
        files = []
    files.sort()
    jd_files = glob('%s_*' % jd_file_base)
    if len(jd_files) == 0:
        jd_files = []
    jd_files.sort()
    context = {
        'id': id,
        'config_files': files,
        'job_desc_files': jd_files,
        'network_selector': {
            'exit_code': int(ns_exit_code)
        }
    }
    with open(json_file, 'w') as f:
        json.dump(context, f, indent=2)
    with open(result_file, 'w') as f:
        f.write(str(files))


if __name__ == "__main__":
    writeContextJson(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
