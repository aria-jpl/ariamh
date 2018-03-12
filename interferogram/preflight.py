#!/usr/bin/env python
import requests


def create_job(objectid, url, projectName, archive_filename, h5_file, sensor, direction,
               track, longitude, startingLatBand, endingLatBand, temporalBaseline,
               doppler, criticalBaseline, coherenceThreshold, wuid=None, job_num=None):
    """Map function for job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    return {
        "job_type": "job:ariamh_preflight",
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "objectid": objectid,
            "url": url,
            "projectName": projectName,
            "archive_filename": archive_filename,
            "h5_file": h5_file,
            "sensor": sensor,
            "direction": direction,
            "track": track,
            "longitude": longitude,
            "startingLatBand": startingLatBand,
            "endingLatBand": endingLatBand,
            "temporalBaseline": temporalBaseline,
            "doppler": doppler,
            "criticalBaseline": criticalBaseline,
            "coherenceThreshold": coherenceThreshold,
            "localize_urls": [{'url': "%s/%s" % (url, archive_filename)}]
        }
    }


def get_preflight_configs(job):
    """Function for extracting preflight config files."""

    job_url = job['job_url']
    r = requests.get('%s/results.json' % job_url, verify=False)
    r.raise_for_status()
    res = r.json()
    if 'coherence' not in res:
        raise RuntimeError("No preflight coherence param file provided by preflight.")
    if 'peg' not in res:
        raise RuntimeError("No preflight peg region file provided by preflight.")
    if len(res['peg']) > 0:
        return ('%s/%s' % (job_url, res['coherence'][0]), '%s/%s' % (job_url, res['peg'][0]))
    else:
        return ('%s/%s' % (job_url, res['coherence'][0]), None)
