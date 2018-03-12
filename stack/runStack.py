#!/usr/bin/env python3
import json
import sys
import os
from utils.contextUtils import toContext
from utils.createImage import createImage

def quickrun(process,command):
    exitV = 0;
    if  os.system(command): 
        message = process + ': failed'
        exitV = 1        
        toContext(process,exitV,message)
        return
    else:
        message = process + ': completed'        
        toContext(process,exitV,message)

def finalize(prdName,meta):
    import shutil
    fp = open('../valid.list')
    pair = fp.readlines()[0].split()[0]
    fp.close()
    #extract velocity form results
    los = 'LOS_velocity.geo'
    command = 'extractVelocity.py -i Stack/TS-PARAMS.h5 -o ' + los + ' -x ../insar/' + pair + '/insarProc.xml'
    process = "extractVelocity"
    quickrun(process,command)
    exitV = 0
    dirName = prdName
    #create product dir
    try:
        os.mkdir(dirName)
    except:
        exitV = 1
        toContext("runStack:finalize",exitV,"Failed to create product directory") 
    #create .met.json
    fp = open(os.path.join(dirName,prdName + '.met.json'),'w')
    json.dump({'losVelocity':los,'interferograms':'ifg.list'},fp,indent=4)
    fp.close()
        
    #create png from velocity and move all the products into the product dir
    try:
        createImage('mdx.py -P '  + los,los)
        productList = ['ifg.list','../' + meta]
        listFiles = os.listdir('./')
        for fl in listFiles:
            if(fl.count('.geo')):
                productList.append(fl)
        
       
        #just in case the default self._inputFile has been modified
        for fileName in productList:
            shutil.move(fileName,dirName)
    except Exception:
        exitV = 1
        toContext("runStack:finalize",exitV,"Failed to create image or moving products to product directory")
    #move the product dir up
    try:
        shutil.move(dirName,'../')
    except Exception:
        toContext("runStack:finalize",exitV,"Failed to move product directory") 

    #move up
    os.system('mv context.json ../')
    os.chdir('../')
    
def main():
    
    ariahome = os.environ['ARIAMH_HOME']
    inputs = json.load(open(sys.argv[1]))
    meta = inputs['metaFile']
    inputFile = os.path.join(ariahome,'conf','velocityMapParams.json')
    processes = ["runStack:getMetadata","runStack:stageInterferograms","runStack:runQA",
                 "runStack:getAuxData","runStack:prepGIAnT_cali"]
    commands = ['getMetadata.py ' + sys.argv[1],'stageInterferograms.py ' + meta,'runQA.py ' + inputFile,
                'getAuxData.py ' + inputFile,'prepGIAnT_cali.py ' + inputFile]
    
    for process,command in zip(processes,commands):
        quickrun(process,command)
    try:
        os.system('cp context.json ./GIAnT')
        os.chdir('./GIAnT')
    except Exception:
        toContext("runStack:chdir",exitV,"Failed chdir to GIAnT") 

    
    processes = ["runStack:prepxml","runStack:PrepIgramStack","runStack:ProcessStack","runStack:TimefnInvert"]

    commands = ['python prepxml.py','PrepIgramStack.py','ProcessStack.py','TimefnInvert.py']
    for process,command in zip(processes,commands):
        quickrun(process,command)
    finalize(inputs['productName'],meta)
   
   

if __name__ == '__main__':
    sys.exit(main())
