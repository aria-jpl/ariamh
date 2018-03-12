#!/usr/bin/env python
import os, sys, json, re, shutil
from subprocess import check_call


WORK_RE = re.compile(r'\d{5}-.+')


def copy_sciflo_work(output_dir):
    """Move over sciflo work dirs."""

    for root, dirs, files in os.walk(output_dir):
        for d in dirs:
            if not WORK_RE.search(d): continue
            path = os.path.join(root, d)
            if os.path.islink(path) and os.path.exists(path):
                real_path = os.path.realpath(path)
                base_name= os.path.basename(real_path)
                new_path = os.path.join(root, base_name)
                shutil.copytree(real_path, new_path)
                os.unlink(path)
                os.symlink(base_name, path)


def extract_error(sfl_json):
    """Extract SciFlo error and traceback for mozart."""

    with open(sfl_json) as f: j = json.load(f)
    exc_message = j.get('exceptionMessage', None)
    if exc_message is not None:
        try: exc_list = eval(exc_message)
        except: exc_list = []
        if len(exc_list) == 3:
            proc = exc_list[0]
            exc = exc_list[1]
            tb = exc_list[2]
            try: exc = eval(exc)
            except: pass
            if isinstance(exc, tuple) and len(exc) == 2:
                err = exc[0]
                job_json = exc[1]
                if isinstance(job_json, dict):
                    if 'job_id' in job_json:
                        err_str = 'SciFlo step %s with job_id %s (task %s) failed: %s' % \
                                  (proc, job_json['job_id'], job_json['uuid'], err)
                        with open('_alt_error.txt', 'w') as f:
                            f.write("%s\n" % err_str)
                        with open('_alt_traceback.txt', 'w') as f:
                            f.write("%s\n" % job_json['traceback'])
            else:
                err_str = 'SciFlo step %s failed: %s' % (proc, exc)
                with open('_alt_error.txt', 'w') as f:
                    f.write("%s\n" % err_str)
                with open('_alt_traceback.txt', 'w') as f:
                    f.write("%s\n" % tb)


def main():
    """Run interferogram stitcher sciflo."""

    # read in _context.json
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    with open('_context.json') as f:
        context = json.load(f)

    # get workflow
    SFL = os.path.join(os.environ['HOME'], 'ariamh', 'interferogram', 'InterferogramStitcher.sf.xml')

    # build sciflo args
    sfl_args = ["context_file=%s" % context_file]

    # build paths to executables
    SFLEXEC_CMD = os.path.join(os.environ['HOME'], 'verdi', 'bin', 'sflExec.py')

    # execute sciflo
    cmd = [SFLEXEC_CMD, "-s", "-f", "-o", "output", "--args", '"%s"' % ','.join(sfl_args), SFL]
    print("Running sflExec.py command:\n%s" % ' '.join(cmd))
    #check_call(cmd, shell)
    status = os.system(' '.join(cmd))
    print("Exit status is: %d" % status)
    if status != 0:
        extract_error('output/sciflo.json')
        status = 1

    # copy sciflo work and exec dir
    try: copy_sciflo_work("output")
    except: pass

    return status


if __name__ == "__main__":
    sys.exit(main())
