#!/usr/bin/env python

import os

######This needs to be custom written for your stacks if needed
def makefnames(dates1,dates2,sensor):
    ####Sensor input is ignored in this case
    dirname = '$relpath'     #Relative path provided. Change to absolute path if needed.
    iname = '%s/%s_%s/corrections_topophase.unw.geo'%(dirname,dates1,dates2)
    if not os.path.exists(iname):
        iname='%s/%s_%s/filt_topophase.unw.geo'%(dirname,dates1,dates2)

    #cname = '%s/%s_%s/topophase.cor.geo'%(dirname,dates1,dates2)
    cname = '%s/%s_%s/phsig.cor.geo'%(dirname, dates1, dates2)

    return iname,cname


#####Assume a simple linear term for Calimap 
def timedict():
    rep = [['POLY',[1],[0.0]]]
    return rep

NSBASdict = timedict
