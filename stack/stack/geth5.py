#!/usr/bin/env python

import os
import argparse
import paramiko
from scp import SCPClient

def createSSHclient(host='pahoehoe', user='agram'):
    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(host, username=user)
    return ssh_client

def parse():
    parser = argparse.ArgumentParser(description='Fetches hdf5 files from pahoehoe.')
    parser.add_argument('-i', nargs='+', dest='flist',
            help='List of EL*.tar.gz files', type=str)
    parser.add_argument('-o', default='.', dest='outdir',
            help='Directory to download in', type=str)
    return parser.parse_args()

if __name__ == '__main__':
    inps = parse()

    currdir = os.getcwd()
    outdir=os.path.relpath(inps.outdir, currdir)

    ssh = createSSHclient()
    scp = SCPClient(ssh.get_transport())

    dirs = ['/mnt/phh-r0a/calimap/data',
            '/mnt/phh-r0a/calimap/data/JPL-Temp']

    for h5 in inps.flist:
        for dir in dirs:
            inname = os.path.join(dir, h5)
            outname = os.path.join(outdir, h5)
            print 'Pahoe: ', inname
            print 'aria1: ', outname
            try:
                scp.get(inname)
            except:
                print 'Didnt find file.'
                pass
            else:
                break



