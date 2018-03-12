#!/usr/bin/env python
import os, sys, fcntl, errno, traceback, time, re, shutil
from glob import glob


PROD_RE = re.compile(r'datastager-(.*?)-')


def janitor(root_work_dir):
    """Clean up large files when no longer needed."""

    ds_dirs = glob("%s/????/??/??/datastager-*" % root_work_dir)
    for ds_dir in ds_dirs:
        match = PROD_RE.search(ds_dir)
        if not match: raise RuntimeError("Failed to extract prod from %s" % ds_dir)

        prod = match.group(1)
        prod_dir = os.path.join(ds_dir, prod)
        if os.path.exists(os.path.join(ds_dir, '.done')):
            if os.path.exists(prod_dir): shutil.rmtree(prod_dir)
            
        
if __name__ == "__main__":

    # lock so that only one instance can run
    lock_file = "/tmp/datastager_janitor.lock"
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
