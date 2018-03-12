#!/usr/bin/env python
import sys, json, types


def add_metadata(metadata_file, hash_id, context_file):
    """Add metadata to json file."""

    with open(metadata_file) as f:
        metadata = json.load(f)

    with open(context_file) as f:
        context = json.load(f)

    # add hash
    metadata['input_hash_id'] = hash_id

    # add tags
    if 'tags' not in metadata:
        metadata['tags'] = []

    # add project
    if 'project' in context:
        metadata['tags'].append(context['project'])

    # add corrections
    ctx_procs = context.get('processes', {})
    for proc_name in ctx_procs:
        if proc_name != 'pyAPSCorrect': continue
        ctx_proc = ctx_procs[proc_name]
        if isinstance(ctx_proc, types.DictType) and 'returnStatus' in ctx_proc:
            ctx_status = ctx_proc['returnStatus']
            if isinstance(ctx_status, types.DictType):
                proc_val = ctx_proc.get('value', None)
                proc_desc = ctx_proc.get('description', "no description specified")
                if proc_val == 0:
                    metadata['corrections'] = [ "pyAPS" ]
                else:
                    metadata['corrections'] = [ "none" ]
                break

    # set dataset_type
    metadata['dataset_type'] = "interferogram"

    # overwrite metadata json file
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("%s <metadata JSON file> <netsel hash ID> <context JSON file>" % sys.argv[0])
        sys.exit(1)

    add_metadata(sys.argv[1], sys.argv[2], sys.argv[3])
    sys.exit(0)
