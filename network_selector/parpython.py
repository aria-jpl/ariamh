#!/usr/bin/env python
import os, requests


def create_job_desc(objectid, project, mode, workflow, unwrapper, unwrap,
                    posting, filterStrength, output_name, geolist, productList,
                    wuid=None, job_num=None):
    """Map function for job description json creation in network selector phase."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    return {
        "job_type": "job:ariamh_job_description",
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "id": objectid,
            "project": project,
            "mode": mode,
            "workflow": workflow,
            "unwrapper": unwrapper,
            "unwrap": unwrap,
            "posting": posting,
            "filterStrength": filterStrength,
            "output_name": output_name,
            "geolist": geolist,
            "productList": productList,
        }
    }


def get_jobdesc_config(job):
    """Function for extracting job description config file."""

    job_desc_url = '%s/job_description.json' % job['job_url']
    r = requests.get(job_desc_url, verify=False)
    r.raise_for_status()
    return job_desc_url


def create_job(objectid, output_name, project, job_desc_url, wuid=None, job_num=None):
    """Map function for job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    return {
        "job_type": "job:ariamh_network_selector",
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "id": objectid,
            "output_name": output_name,
            "project": project,
            "job_desc_url": job_desc_url,
            "job_desc_file": os.path.basename(job_desc_url),
        }
    }


def create_job_with_cfgs(objectid, output_name, project, job_desc_url, coherence_url, peg_url, wuid=None, job_num=None):
    """Map function for job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    return {
        "job_type": "job:ariamh_network_selector",
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "id": objectid,
            "output_name": output_name,
            "project": project,
            "coherence_url": coherence_url,
            "peg_url": peg_url,
            "job_desc_url": job_desc_url,
            "job_desc_file": os.path.basename(job_desc_url),
        }
    }


def get_netsel_configs(job):
    """Function for extracting network selector config files."""

    job_url = job['job_url']
    r = requests.get('%s/context.json' % job_url, verify=False)
    r.raise_for_status()
    res = r.json()
    if 'config_files' not in res:
        raise RuntimeError("No interferogram json files provided by network select.")
    if 'job_desc_files' not in res:
        raise RuntimeError("No job description json files provided by network select.")
    config_files = []
    for i in res['config_files']:
        config_files.append('%s/%s' % (job_url, i))
    job_desc_files = [] 
    for i in res['job_desc_files']:
        job_desc_files.append('%s/%s' % (job_url, i))
    return (config_files, job_desc_files)
