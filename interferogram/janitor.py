#!/usr/bin/env python
import os, sys, fcntl, errno, traceback, time
from glob import glob


def janitor(root_work_dir):
    """Clean up large files when no longer needed."""

    ci_dirs = glob("%s/????/??/??/create_interferogram-*" % root_work_dir)
    for ci_dir in ci_dirs:
        # get h5 files
        h5_files = glob("%s/*.h5" % ci_dir)

        # h5 files can be deleted if *.slc exists
        slc_files = glob("%s/*.slc" % ci_dir)
        if len(slc_files) > 0:
            for h5_file in h5_files:
                os.unlink(h5_file)

        # get raw files
        raw_files = glob("%s/*.raw*" % ci_dir)

        # raw files can be deleted if *.int exists
        int_files = glob("%s/*.int" % ci_dir)
        if len(int_files) > 0:
            for raw_file in raw_files:
                os.unlink(raw_file)
       
        # slc files can be deleted if *.rdr exists
        rdr_files = glob("%s/*.rdr" % ci_dir)
        if len(rdr_files) > 0:
            for slc_file in slc_files:
                os.unlink(slc_file)


if __name__ == "__main__":

    # lock so that only one instance can run
    lock_file = "/tmp/janitor.lock"
    f = open(lock_file, 'w')
    f.write("%d\n" % os.getpid())
    f.flush()
    try: fcntl.lockf(f, fcntl.LOCK_EX|fcntl.LOCK_NB)
    except IOError, e:
        if e.errno == errno.EAGAIN:
            sys.stderr.write("Janitor is already running.\n")
            sys.exit(-1)
        raise

    # run cleanup
    root_work_dir = "/data/work/jobs"
    try: janitor(root_work_dir)
    finally:
        f.close()
        os.unlink(lock_file)
