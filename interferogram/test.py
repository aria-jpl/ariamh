#!/usr/bin/env python
import createInterferogram as CI
import os
import shutil
import json
from datetime import datetime as dt
def main():
    th = CI.InsarThread(1,1,'alos',1,1)
    import pdb
    pdb.set_trace()
    th._imageCorners = [1,2,3,4]
    th._newDir = '/data/interferogram_0/'
    jsonD = th.createProductJson('/data/interferogram_0/')
    dirName = jsonD['name'] + '-' + jsonD['type'] + '_' + dt.now().isoformat()
    try:
        os.mkdir(dirName)
    except:
        pass
    for fileName in th._productList:
        shutil.move(os.path.join(th._newDir,fileName),dirName)
    
    fp = open(dirName + '.json','w')
    json.dump(jsonD,fp)
    fp.close()


if __name__ == '__main__':
    import sys
    sys.exit(main())
