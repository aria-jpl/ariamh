#!/usr/bin/env python
import os, requests, json
from fetchOrbitES import fetch



def create_ifg_job(project, stitched, auto_bbox, ifg_id, master_zip_url, master_orbit_url, 
                   slave_zip_url, slave_orbit_url, swathnum, bbox, wuid=None, job_num=None):
    """Map function for create interferogram job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    # set job type and disk space reqs
    if stitched:
        job_type = "sentinel_ifg-stitched"
        disk_usage = "300GB"
    else:
        job_type = "sentinel_ifg-singlescene"
        disk_usage = "200GB"

    # set job queue based on project
    job_queue = "%s-job_worker-large" % project
    job_queue = "standard_product-s1gunw-topsapp"

    # set localize urls
    localize_urls = [
        { 'url': master_orbit_url },
        { 'url': slave_orbit_url },
    ]
    for m in master_zip_url: localize_urls.append({'url': m})
    for s in slave_zip_url: localize_urls.append({'url': s})

    return {
        "job_name": "%s-%s" % (job_type, ifg_id),
        "job_type": "job:%s" % job_type,
        "job_queue": job_queue,
        "container_mappings": {
            "$HOME/.netrc": "/home/ops/.netrc",
            "$HOME/.aws": "/home/ops/.aws",
            "$HOME/verdi/etc/settings.conf": "/home/ops/ariamh/conf/settings.conf"
        },    
        "soft_time_limit": 86400,
        "time_limit": 86700,
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "project": project,
            "id": ifg_id,
            "master_zip_url": master_zip_url,
            "master_zip_file": [os.path.basename(i) for i in master_zip_url],
            "master_orbit_url": master_orbit_url,
            "master_orbit_file": os.path.basename(master_orbit_url),
            "slave_zip_url": slave_zip_url,
            "slave_zip_file": [os.path.basename(i) for i in slave_zip_url],
            "slave_orbit_url": slave_orbit_url,
            "slave_orbit_file": os.path.basename(slave_orbit_url),
            "swathnum": swathnum,
            "bbox": bbox,
            "auto_bbox": auto_bbox,

            # v2 cmd
            "_command": "/home/ops/ariamh/interferogram/sentinel/create_ifg.sh",

            # disk usage
            "_disk_usage": disk_usage,

            # localize urls
            "localize_urls": localize_urls,
        }
    } 


def create_standard_product_job(project, stitched_arg, auto_bbox, ifg_id, master_zip_url, master_orbit_url, 
		   slave_zip_url, slave_orbit_url, swathnums, bbox, dem_type, job_priority, master_scenes, slave_scenes, union_geojson, 
                   ifg_hash, platform, direction, west_lat, track, orbit_type, master_ifg_dt, slave_ifg_dt, wuid=None, job_num=None):
    """Map function for create standard_product interferogram job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    job_type = "sentinel_standard-product-ifg-singlescene"
    disk_usage = "300GB"

    if stitched_arg:
        job_type = "sentinel_ifg-stitched"
        disk_usage = "300GB"
    else:
        job_type = "sentinel_ifg-singlescene"
        disk_usage = "200GB"


    # set job queue based on project
    job_queue = "standard_product_s1ifg-s1ifg_single_scene"
    job_queue = "standard_product-s1gunw-topsapp"

    #job_queue = "factotum-job_worker-small"
    # set localize urls
    localize_urls = [
        { 'url': master_orbit_url },
        { 'url': slave_orbit_url },
    ]
    for m in master_zip_url: localize_urls.append({'url': m})
    for s in slave_zip_url: localize_urls.append({'url': s})

    return {
        "job_name": "%s-%s" % (job_type, ifg_id),
        "job_type": "job:%s" % job_type,
        "job_queue": job_queue,
        "container_mappings": {
            "/home/ops/.netrc": "/home/ops/.netrc",
            "/home/ops/.aws": "/home/ops/.aws",
            "/home/ops/verdi/etc/settings.conf": "/home/ops/ariamh/conf/settings.conf"
        },    
        "soft_time_limit": 7200,
        "time_limit": 7800,
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "project": project,
            "id": ifg_id,
            "master_zip_url": master_zip_url,
            "master_zip_file": [os.path.basename(i) for i in master_zip_url],
            "master_orbit_url": master_orbit_url,
            "master_orbit_file": os.path.basename(master_orbit_url),
            "slave_zip_url": slave_zip_url,
            "slave_zip_file": [os.path.basename(i) for i in slave_zip_url],
            "slave_orbit_url": slave_orbit_url,
            "slave_orbit_file": os.path.basename(slave_orbit_url),
            "swathnum": [1,2,3],
	    "azimuth_looks": 7,
  	    "range_looks" : 19,
	    "singlesceneOnly": True,
 	    "covth": 0.99,
	    "dem_type": dem_type,
	    "filter_strength": 0.5,
	    "job_priority": job_priority,
            "bbox": bbox,
            "auto_bbox": auto_bbox,
            "slave_scenes" : slave_scenes,
            "master_scenes" : master_scenes,
            "union_geojson" : union_geojson,
            "platform" : platform,
            "direction" : direction,
            "ifg_hash" : ifg_hash,
            "west_lat" : west_lat,
            "track" : track,
            "orbit_type" : orbit_type,
            "master_ifg_dt" : master_ifg_dt,
            "slave_ifg_dt" : slave_ifg_dt,

            # v2 cmd
            "_command": "/home/ops/ariamh/interferogram/sentinel/create_ifg_standard_product.sh",

            # disk usage
            "_disk_usage": disk_usage,

            # localize urls
            "localize_urls": localize_urls,
        }
    } 
def create_offset_job(project, stitched, auto_bbox, ifg_id, master_zip_url, master_orbit_url, 
                   slave_zip_url, slave_orbit_url, swathnum, bbox, ampcor_skip_width, ampcor_skip_height,
                   ampcor_src_win_width, ampcor_src_win_height, ampcor_src_width, ampcor_src_height,
                   dem_urls, wuid=None, job_num=None):
    """Map function for create interferogram job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    # set job type and disk space reqs
    if stitched:
        job_type = "sentinel_offset-stitched"
        disk_usage = "300GB"
    else:
        job_type = "sentinel_offset-singlescene"
        disk_usage = "200GB"

    # set job queue based on project
    job_queue = "%s-job_worker-large" % project

    # set localize urls
    localize_urls = [
        { 'url': master_orbit_url },
        { 'url': slave_orbit_url },
    ]
    for m in master_zip_url: localize_urls.append({'url': m})
    for s in slave_zip_url: localize_urls.append({'url': s})

    return {
        "job_name": "%s-%s" % (job_type, ifg_id),
        "job_type": "job:%s" % job_type,
        "job_queue": job_queue,
        "container_mappings": {
            "$HOME/.netrc": "/home/ops/.netrc",
            "$HOME/.aws": "/home/ops/.aws",
            "$HOME/verdi/etc/settings.conf": "/home/ops/ariamh/conf/settings.conf"
        },    
        "soft_time_limit": 86400,
        "time_limit": 86700,
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "project": project,
            "id": ifg_id,
            "master_zip_url": master_zip_url,
            "master_zip_file": [os.path.basename(i) for i in master_zip_url],
            "master_orbit_url": master_orbit_url,
            "master_orbit_file": os.path.basename(master_orbit_url),
            "slave_zip_url": slave_zip_url,
            "slave_zip_file": [os.path.basename(i) for i in slave_zip_url],
            "slave_orbit_url": slave_orbit_url,
            "slave_orbit_file": os.path.basename(slave_orbit_url),
            "swathnum": swathnum,
            "bbox": bbox,
            "auto_bbox": auto_bbox,
            "ampcor_skip_width": ampcor_skip_width,
            "ampcor_skip_height": ampcor_skip_height,
            "ampcor_src_win_width": ampcor_src_win_width,
            "ampcor_src_win_height": ampcor_src_win_height,
            "ampcor_src_width": ampcor_src_width,
            "ampcor_src_height": ampcor_src_height,
            "dem_urls": dem_urls,

            # v2 cmd
            "_command": "/home/ops/ariamh/interferogram/sentinel/create_offset.sh",

            # disk usage
            "_disk_usage": disk_usage,

            # localize urls
            "localize_urls": localize_urls,
        }
    } 

def create_rsp_job(project, stitched, auto_bbox, rsp_id, master_zip_url, master_orbit_url, 
                   slave_zip_url, slave_orbit_url, swathnum, bbox, wuid=None, job_num=None):
    """Map function for create slc_pair product job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    # set job type and disk space reqs
    if stitched:
        job_type = "sentinel_rsp-stitched"
        disk_usage = "300GB"
    else:
        job_type = "sentinel_rsp-singlescene"
        disk_usage = "200GB"

    # set job queue based on project
    job_queue = "%s-job_worker-large" % project

    # set localize urls
    localize_urls = [
        { 'url': master_orbit_url },
        { 'url': slave_orbit_url },
    ]
    for m in master_zip_url: localize_urls.append({'url': m})
    for s in slave_zip_url: localize_urls.append({'url': s})

    return {
        "job_name": "%s-%s" % (job_type, rsp_id),
        "job_type": "job:%s" % job_type,
        "job_queue": job_queue,
        "container_mappings": {
            "$HOME/.netrc": "/home/ops/.netrc",
            "$HOME/.aws": "/home/ops/.aws",
            "$HOME/verdi/etc/settings.conf": "/home/ops/ariamh/conf/settings.conf"
        },    
        "soft_time_limit": 86400,
        "time_limit": 86700,
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "project": project,
            "id": rsp_id,
            "master_zip_url": master_zip_url,
            "master_zip_file": [os.path.basename(i) for i in master_zip_url],
            "master_orbit_url": master_orbit_url,
            "master_orbit_file": os.path.basename(master_orbit_url),
            "slave_zip_url": slave_zip_url,
            "slave_zip_file": [os.path.basename(i) for i in slave_zip_url],
            "slave_orbit_url": slave_orbit_url,
            "slave_orbit_file": os.path.basename(slave_orbit_url),
            "swathnum": swathnum,
            "bbox": bbox,
            "auto_bbox": auto_bbox,

            # v2 cmd
            "_command": "/home/ops/ariamh/interferogram/sentinel/create_rsp.sh",

            # disk usage
            "_disk_usage": disk_usage,

            # localize urls
            "localize_urls": localize_urls,
        }
    } 


def create_xtstitched_ifg_job(project, stitched, auto_bbox, ifg_id, master_zip_url, master_orbit_url, 
                              slave_zip_url, slave_orbit_url, bbox, wuid=None, job_num=None):
    """Map function for create cross-track stitched interferogram job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    # set job type and disk space reqs
    if stitched:
        job_type = "sentinel_ifg-stitched"
    else:
        job_type = "sentinel_ifg-singlescene"

    # set job queue based on project
    job_queue = "%s-job_worker-large" % project

    # set localize urls
    localize_urls = [
        { 'url': master_orbit_url },
        { 'url': slave_orbit_url },
    ]
    for m in master_zip_url: localize_urls.append({'url': m})
    for s in slave_zip_url: localize_urls.append({'url': s})



    print("job_name: %s-%s" % (job_type, ifg_id))
    return {
        "job_name": "%s-%s" % (job_type, ifg_id),
        "job_type": "job:%s" % job_type,
        "job_queue": job_queue,
        "container_mappings": {
            "$HOME/.netrc": "/home/ops/.netrc",
            "$HOME/.aws": "/home/ops/.aws",
            "$HOME/verdi/etc/settings.conf": "/home/ops/ariamh/conf/settings.conf"
        },    
        "soft_time_limit": 86400,
        "time_limit": 86700,
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "project": project,
            "id": ifg_id,
            "master_zip_url": master_zip_url,
            "master_zip_file": [os.path.basename(i) for i in master_zip_url],
            "master_orbit_url": master_orbit_url,
            "master_orbit_file": os.path.basename(master_orbit_url),
            "slave_zip_url": slave_zip_url,
            "slave_zip_file": [os.path.basename(i) for i in slave_zip_url],
            "slave_orbit_url": slave_orbit_url,
            "slave_orbit_file": os.path.basename(slave_orbit_url),
            "swathnum": None,
            "bbox": bbox,
            "auto_bbox": auto_bbox,

            # v2 cmd
            "_command": "/home/ops/ariamh/interferogram/sentinel/create_ifg.sh",

            # disk usage
            "_disk_usage": "500GB",

            # localize urls
            "localize_urls": localize_urls,
        }
    }
