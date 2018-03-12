#!/usr/bin/env python
import os


def create_job(netsel_url, jobdesc_url, project, wuid=None, job_num=None):
    """Map function for job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    netsel_file = os.path.basename(netsel_url)
    jobdesc_file = os.path.basename(jobdesc_url)

    return {
        "job_type": "job:ariamh_create_interferogram",
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "project": project,
            "netsel_file": netsel_file,
            "netsel_url": netsel_url,
            "jobdesc_file": jobdesc_file,
            "jobdesc_url": jobdesc_url,
        }
    }


def create_job_with_cfgs(netsel_url, jobdesc_url, project, coherence_url, peg_url, wuid=None, job_num=None):
    """Map function for job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    netsel_file = os.path.basename(netsel_url)
    jobdesc_file = os.path.basename(jobdesc_url)

    return {
        "job_type": "job:ariamh_create_interferogram",
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "project": project,
            "netsel_file": netsel_file,
            "netsel_url": netsel_url,
            "jobdesc_file": jobdesc_file,
            "jobdesc_url": jobdesc_url,
            "coherence_url": coherence_url,
            "peg_url": peg_url,
        }
    }


def stitch_ifg_job(project, direction, extra_products, filenames, filename_urls,
                   ifg_id, wuid=None, job_num=None):
    """Map function for interferogram stitcher job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    # set job type
    job_type = "stitch_ifgs"

    # set job queue based on project
    job_queue = "%s-job_worker-large" % project

    # set localize urls
    localize_urls = filename_urls

    return {
        "job_name": "%s-%s" % (job_type, ifg_id),
        "job_type": "job:%s" % job_type,
        "job_queue": job_queue,
        "container_mappings": {
            "/home/ops/.netrc": "/home/ops/.netrc",
            "/home/ops/.aws": "/home/ops/.aws",
            "/home/ops/ariamh/conf/settings.conf": "/home/ops/ariamh/conf/settings.conf"
        },    
        "soft_time_limit": 86400,
        "time_limit": 86700,
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "project": project,
            "direction": direction,
            "extra_products": extra_products,
            "filenames": filenames,
            "id": ifg_id,

            # v2 cmd
            "_command": "/home/ops/ariamh/interferogram/stitch_ifgs.sh",

            # disk usage
            "_disk_usage": "300GB",

            # localize urls
            "localize_urls": localize_urls,
        }
    } 
