#!/usr/bin/env python
import os, sys, shutil
from fabric.api import env, get, run, put
from fabric.contrib.files import exists


def move_remote_path(host, src, dest):
    """Move remote directory safely."""

    env.host_string = host
    env.abort_on_prompts = True
    dest_dir = os.path.dirname(dest)
    if exists(dest):
        run("rm -rf %s" % dest)
    if not exists(dest_dir):
        run("mkdir -p %s" % dest_dir)
    ret = run("mv -f %s %s" % (src, dest)) 
    return ret 


def remove_remote_path(host, path):
    """Remove path."""

    env.host_string = host
    env.abort_on_prompts = True
    run("rm -rf %s" % path)
    

def remove_remote_path_glob(host, path):
    """Remove path glob."""

    env.host_string = host
    env.abort_on_prompts = True
    run("rm -rf %s/*" % path)


def upload_product_for_ingest(host, local_path, prod_path):
    """Upload prod for ingest."""

    env.host_string = host
    env.abort_on_prompts = True
    remove_remote_path(host, prod_path)
    upload_path = os.path.dirname(prod_path)
    if not exists(upload_path):
        run("mkdir -p %s" % upload_path)
    put(local_path, upload_path, mirror_local_mode=True)
    done_file = prod_path + '.done'
    run("touch %s" % done_file)


def ls(host, path):
    """ls."""

    env.host_string = host
    env.abort_on_prompts = True
    run("ls %s" % path)
    

def ls_la(host, path):
    """ls -la."""

    env.host_string = host
    env.abort_on_prompts = True
    run("ls -la %s" % path)
    

def get_remote(host, rpath):
    """Get remote dir/file."""

    env.host_string = host
    env.abort_on_prompts = True
    lpath = os.path.abspath('./%s' % os.path.basename(rpath))
    if os.path.exists(lpath):
        if os.path.isdir(lpath): shutil.rmtree(lpath)
        else: os.unlink(lpath)
    r = get(rpath, '.')
    return lpath
