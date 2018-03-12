#!/usr/bin/env python3
import json
import argparse
import os
import shutil
import sys
from datetime import datetime

def getUrlList(url, id, opt=True):
    '''
    Get URL list of files using webDAV.
    '''
    fList = []
    prodList = ['filt_topophase.unw.geo', 'phsig.cor.geo']
    otherList = ['insarProc.xml', 'browse.png']
    optList = ['los.rdr.geo']

    for kk in prodList:
        fList.append((url, kk))
        fList.append((url, kk+'.xml'))

    for kk in otherList:
        fList.append((url, kk))

    if opt:
        for kk in optList:
            fList.append((url, kk))
            fList.append((url, kk+'.xml'))
    
    return fList

def download_file(url, path, user=None, pw=None):
    '''
    Download file for input.
    '''
    from utils.UrlUtils import UrlUtils
    uu = UrlUtils()
    return True if uu.download(url,path, user, pw) == 0 else False
    '''
    session = requests.session()
    session.auth = (user, pw)
    request = session.get(url, stream=True, verify=False)

    try:
#        print 'Url: ', url
#        print 'Path: ', path
        val = request.raise_for_status()
#        print 'Status: ', val
        success = True
    except:
        success = False

    if success:
        with open(path,'wb') as f:
            for chunk in request.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()

    return success
    '''
def parse():
    '''
    Command Line Parser.
    '''
    parser = argparse.ArgumentParser(description='Stages interferograms with information provided in a json file.')
    parser.add_argument('--meta', dest='inlist', type=str, required=True,
            help='Name of the json file with the list of interferograms.')
    parser.add_argument('--user', dest='user', type=str, required=False,
            help='JPL user name')
    parser.add_argument('--pw', dest='passwd', type=str, required=False,
            help='JPL password')

    return parser.parse_args()


if __name__ == '__main__':
    '''
    The main driver.
    '''

    #Parse command line
    #inps = parse()
    
    #Parse input json
    inObj = json.load(open(sys.argv[1]))

    print('Number of interferograms to stage: ', len(inObj))
    listIfgs = inObj

    currDir = os.getcwd()

    ####Create an insar subdir to stage interferograms
    insarDir = os.path.join(currDir, 'insar')
    print('insarDir : ', insarDir)
    if not os.path.exists(insarDir):
        os.mkdir(insarDir)
    else:
        print('insar Directory %s already exists.'%(insarDir))

    countSuccess = 0

    ###Loop over dirs
    for index,elem in enumerate(listIfgs):
        print('Staging interferogram: ', index+1)
        url = elem['url']
        id = elem['id']
        masterDate = datetime.strptime(elem['sensingStart'][0], "%Y-%m-%dT%H:%M:%S.%f")
        slaveDate = datetime.strptime(elem['sensingStart'][1], "%Y-%m-%dT%H:%M:%S.%f")
        dirList = getUrlList(url, id, opt= (index==0))
        stageDir = os.path.join(insarDir, masterDate.strftime('%Y%m%d')+'_'+slaveDate.strftime('%Y%m%d'))

        if slaveDate > masterDate:
            print('Skipping %s dir - Slave > Master'%(stageDir))
            continue
        
        print('Staging data in directory: %s'%(stageDir))


        if not os.path.exists(stageDir):
            os.mkdir(stageDir)
        else:
            print('Staging directory %s already exists.'%(stageDir))

        success = True
        os.chdir(stageDir)
        for url,file in dirList:
            success = (success and download_file(url,file))
            if not success:
                print('Unable to download: ', file)
                break

        os.chdir(currDir)
        if not success:
            print('Removing directory: ', stageDir)
            shutil.rmtree(stageDir)
        else:
            countSuccess += 1

        os.chdir(currDir)

    print('Number of directories successfully staged: ', countSuccess)
