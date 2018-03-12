#!/usr/bin/env python
import os, sys, json, requests, copy, math
from pprint import pprint, pformat

from frameMetadata.FrameMetadata import FrameMetadata
from utils.UrlUtils import UrlUtils
from utils.queryBuilder import postQuery, buildQuery, createMetaObjects



def check_reference(dataset, md):
    """Check reference of this metadata against what's in GRQ."""

    # get config
    uu = UrlUtils()
    rest_url = uu.rest_url
    
    # is this scene a reference?
    fm_md = copy.deepcopy(md)
    fm = FrameMetadata()
    fm.load(fm_md)

    #sys.stderr.write("fm.reference: %s\n" % fm.reference)
    #sys.stderr.write("fm.trackNumber: %s\n" % fm.trackNumber)
    #sys.stderr.write("fm.beamID: %s\n" % fm.beamID)
    #sys.stderr.write("fm.latitudeIndexMin: %s\n" % fm.latitudeIndexMin)
    #sys.stderr.write("fm.latitudeIndexMax: %s\n" % fm.latitudeIndexMax)

    # if not a reference, save
    if fm.reference == False:
        return { 'ok_to_save': True, 'suspicious_flag': False, 'suspicious_code': '' }

    # check if reference exists already
    extremes = fm.getExtremes(fm.bbox)
    latMin = extremes[0]
    latMax = extremes[1]
    lonMin = extremes[2]
    lonMax = extremes[3]
    latDelta = (latMax - latMin) / 3.
    latitudeResolution = .1
    params = {
        'sensor': fm.platform,
        'dataset_type':dataset,
        'trackNumber':fm.trackNumber,
        'latitudeIndexMin': int(math.floor((latMin - latDelta)/latitudeResolution)),
        'latitudeIndexMax': int(math.ceil((latMax + latDelta)/latitudeResolution)),
        'system_version':uu.version,
        'direction':fm.direction,
        'lookDirection':fm.lookDirection,
        'reference':True,
    }
    if fm.beamID:
        params['beamID'] =  fm.beamID
    metList, status = postQuery(buildQuery(params, ['within']))
    metObj = createMetaObjects(metList)

    # if none found, save
    if len(metObj) == 0:
        return { 'ok_to_save': True, 'suspicious_flag': False, 'suspicious_code': '' }

    # loop over frames and check if in this frame's bbox
    inbbox_count = 0
    frames = []
    for met_idx, tmp_fm in enumerate(metObj):
        inbbox = fm.isInBbox(tmp_fm.refbbox)
        if inbbox: inbbox_count += 1
        frames.append({
            'id': os.path.splitext(metList[met_idx]['dfdn']['ProductName'])[0],
            'archive_filename': metList[met_idx]['archive_filename'],
            'inbbox': inbbox,
        })

    #print "true_count:", true_count

    # if all not in bbox, okay to save but flag suspicious
    if inbbox_count == 0:
        return { 'ok_to_save': True, 
                 'frames': frames,
                 'suspicious_flag': True, 
                 'suspicious_code': 'no_frames_in_bbox' }
    
    # if one is in bbox, not okay to update
    elif inbbox_count == 1:
        #return { 'ok_to_save': False, 'reprocess': True, 'suspicious_flag': True, 'suspicious_code': 'one_frame_in_bbox' }
        # fail for now; it can be eventually reprocessed after the initial re-ingest
        return { 'ok_to_save': False, 
                 'frames': frames,
                 'reprocess': False, 
                 'suspicious_flag': True, 
                 'suspicious_code': 'one_frame_in_bbox' }
    
    # if more than one is in bbox, not okay to update and flag
    else:
        return { 'ok_to_save': False, 
                 'frames': frames,
                 'reprocess': False, 
                 'suspicious_flag': True, 
                 'suspicious_code': 'more_than_one_frame_in_bbox' }
    

if __name__ == "__main__":
    dataset = sys.argv[1]
    md_file = sys.argv[2]
    json_file = sys.argv[3]
    with open(md_file) as f:
        md = json.load(f)
    with open(json_file, 'w') as f:
        f.write(json.dumps(check_reference(dataset, md), indent=2))
