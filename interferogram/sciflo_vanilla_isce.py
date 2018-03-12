#!/usr/bin/env python
import os, sys, json
from subprocess import check_call


def main():
    """Run vanilla ISCE."""

    # read in context.json
    context_file = "context.json"
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    with open('context.json') as f:
        context = json.load(f)

    # get mode and choose workflow
    mode = context['mode']
    if mode == 'nominal':
        SFL = os.path.join(os.environ['HOME'], 'ariamh', 'interferogram', 'VanillaISCE-nominal.sf.xml')
    else:
        SFL = os.path.join(os.environ['HOME'], 'ariamh', 'interferogram', 'VanillaISCE-on_demand.sf.xml')

    # build sciflo args
    flow_args = ['sensor', 'project', 'demURL', 'unwrap', 'unwrapper', 'posting',
                 'geolist', 'productList', 'filterStrength', 'criticalBaseline',
                 'doppler', 'temporalBaseline', 'coherenceThreshold', 'track',
                 'startingLatBand', 'endingLatBand', 'direction', 'longitude',
                 'archive_filename', 'h5_file', 'objectid', 'output_name', 'url']
    sfl_args = []
    for flow_arg in flow_args:
        if flow_arg in context:
            sfl_args.append("%s=%s" % (flow_arg, context[flow_arg]))

    # build paths to executables
    SFLEXEC_CMD = os.path.join(os.environ['HOME'], 'verdi', 'bin', 'sflExec.py')

    # execute sciflo
    cmd = [SFLEXEC_CMD, "-s", "-f", "-o", "output", "--args", '"%s"' % ','.join(sfl_args), SFL]
    print("Running sflExec.py command:\n%s" % ' '.join(cmd))
    #check_call(cmd, shell)
    status = os.system(' '.join(cmd))
    print("Exit status is: %d" % status)
    if status != 0: status = 1
    return status


if __name__ == "__main__":
    sys.exit(main())
