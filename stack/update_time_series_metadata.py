#!/usr/bin/env python
import sys, json, types, hashlib


def add_metadata(ifg_metadata_file, prod_metadata_file, id):
    """Add metadata to json file."""

    with open(ifg_metadata_file) as f:
        ifg_metadata = json.load(f)

    with open(prod_metadata_file) as f:
        prod_metadata = json.load(f)

    # update
    if len(ifg_metadata) > 0:
        prod_metadata.update(ifg_metadata[0])

    # get sensing start and stop times
    start_times = []
    stop_times = []
    for md in ifg_metadata:
        start_times.extend(md['sensingStart'])
        stop_times.extend(md['sensingStop'])
    start_times.sort()
    stop_times.sort()
    prod_metadata['sensing_time_initial'] = start_times[0]
    prod_metadata['sensing_time_final'] = stop_times[-1]

    # overwrite metadata
    prod_metadata['tags'] = [ "interferogram", "time-series" ]
    prod_metadata['product_type'] = "time-series"
    prod_metadata['id'] = id
    m = hashlib.md5()
    m.update(id)
    prod_metadata['input_hash_id'] = m.hexdigest()

    # remove metadata
    if 'url' in prod_metadata: del prod_metadata['url']
    if 'sensingStart' in prod_metadata: del prod_metadata['sensingStart']
    if 'sensingStop' in prod_metadata: del prod_metadata['sensingStop']
    if 'spacecraftName' in prod_metadata: del prod_metadata['spacecraftName']
    if 'horizontalBaseline' in prod_metadata: del prod_metadata['horizontalBaseline']
    if 'totalBaseline' in prod_metadata: del prod_metadata['totalBaseline']
    if 'verticalBaseline' in prod_metadata: del prod_metadata['verticalBaseline']
    if 'orbitNumber' in prod_metadata: del prod_metadata['orbitNumber']
    if 'orbit' in prod_metadata: del prod_metadata['orbit']

    # overwrite product metadata json file
    with open(prod_metadata_file, 'w') as f:
        json.dump(prod_metadata, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("%s <IFG metadata JSON> <time-series metadata JSON> <time-series product ID>" % sys.argv[0])
        sys.exit(1)

    add_metadata(sys.argv[1], sys.argv[2], sys.argv[3])
    sys.exit(0)
