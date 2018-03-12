#!/usr/bin/env python
import os, sys, argparse, tarfile, json
from subprocess import check_call
from glob import glob


def main():
    """Run vanilla ISCE."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--results', help='results json file')
    parser.add_argument('--sensor', nargs='*', help='sensor')
    parser.add_argument('--direction', nargs='*', help='satellite direction')
    parser.add_argument('--doppler', help='doppler')
    parser.add_argument('--track', nargs='*', help='satellite track')
    parser.add_argument('--projectName', help="project name")
    parser.add_argument('--archive_filename', help="archive filename")
    parser.add_argument('--startingLatBand', nargs='*', help='peg region starting latitude')
    parser.add_argument('--longitude', nargs='*', help='peg region longitude')
    parser.add_argument('--h5_file', help="Input H5 file")
    parser.add_argument('--endingLatBand', nargs='*', help='peg region ending latitude')
    parser.add_argument('--criticalBaseline', help='critical perpendicular baseline')
    parser.add_argument('--temporalBaseline', help='critical temporal baseline')
    parser.add_argument('--coherenceThreshold', help='coherence threshold')

    args = parser.parse_args()
    print "sensor:", args.sensor
    print "direction:", args.direction
    print "doppler:", args.doppler
    print "track:", args.track
    print "projectName:", args.projectName
    print "archive_filename:", args.archive_filename
    print "startingLatBand:", args.startingLatBand
    print "longitude:", args.longitude
    print "h5_file:", args.h5_file
    print "endingLatBand:", args.endingLatBand
    print "criticalBaseline:", args.criticalBaseline
    print "temporalBaseline:", args.temporalBaseline
    print "coherenceThreshold:", args.coherenceThreshold

    # build paths to executables
    ARIAMH_HOME = os.path.join(os.environ['HOME'], 'ariamh')
    COHERENCE_CMD = os.path.join(ARIAMH_HOME, 'utils', 'onFlightCoherenceParams.py')
    PEG_CMD = os.path.join(ARIAMH_HOME, 'utils', 'onFlightPeg.py')

    # extract H5 file from tarball
    with tarfile.open(args.archive_filename) as t:
        t.extract('./%s' % args.h5_file)

    # remove tarball
    os.unlink(args.archive_filename)

    # get coherence
    cmd = ["python", COHERENCE_CMD, "-p", args.projectName]
    if args.temporalBaseline != "default": cmd.extend(["-t", args.temporalBaseline]) 
    if args.doppler != "default": cmd.extend(["-d", args.doppler]) 
    if args.criticalBaseline != "default": cmd.extend(["-b", args.criticalBaseline]) 
    if args.coherenceThreshold != "default": cmd.extend(["-c", args.coherenceThreshold]) 
    print("Running coherence command:\n%s" % ' '.join(cmd))
    check_call(cmd)
    coherence_files = glob('coherenceParams_*.json')

    # get peg
    if args.projectName.endswith('sf'): peg_files = []
    else:
        for sensor in args.sensor:
            cmd = ["python", PEG_CMD, "-p", args.projectName, "-n", sensor, "-t", ' '.join(args.track),
                   "-s", ' '.join(args.startingLatBand), "-e", ' '.join(args.endingLatBand),
                   "-l", ' '.join(args.longitude), "-d", ' '.join(args.direction)]
            print("Running peg command:\n%s" % ' '.join(cmd))
            check_call(cmd)
        peg_files = glob('pegfile_*')

    # write results
    with open(args.results, 'w') as f:
        json.dump({'coherence': coherence_files,
                   'peg': peg_files}, f, indent=2)

    # remove h5 file
    os.unlink(args.h5_file)


if __name__ == "__main__":
    main()
    sys.exit(0)
