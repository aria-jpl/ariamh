#!/usr/bin/env python3

from __future__ import division
from __future__ import absolute_import
from builtins import str
from past.utils import old_div
import os 
import sys
import numpy as np
import lxml.objectify as OB
from . import stackSetup as SS
from . import templateSetup as temp
import json

def Seconds(instr):
    vals = instr.split(':')
    secs = float(vals[0]) * 3600 + float(vals[1]) * 60 + float(vals[2])
    return secs


def getIncAngle(rng, ht, re):
    '''
    Compute the incidence angle.
    '''
    sat = (re+ht)

    cosang = old_div(((rng*rng) + (re*re) - (sat*sat)),(2.0*rng*re))
    return np.degrees(np.arccos(cosang)) - 90.0

def parse():
    '''
    Command line parser.
    '''
    '''
    parser = argparse.ArgumentParser(description='Prepare geocoded ISCE stack for GIAnT processing.')
    parser.add_argument('-i', action='store', default='./insar', dest='srcDir',
        help='Directory with IFGs as subdirs', type=str)
    parser.add_argument('-o', action='store', default='.', dest='prepDir',
        help='Directory to prepare GIAnT files in.', type=str)
    parser.add_argument('--ilist', action='store', default='', dest='ilist',
        help='Use specified subset of IFGS.', type=str)
    parser.add_argument('--template', action='store',default='', dest='tempDir',
        help='Directory to pick up GIAnT templates from.', type=str)
    parser.add_argument('--force', action='store_true', default=False, 
        dest='force', help='Force updating of input files for GIAnT.')
    parser.add_argument('--ref', action='store', required=True,
        help='2 Line text file with line number / pixel number in geocoded images  of reference region', dest='ref', type=str)
    inps = parser.parse_args()
    '''
    
    return json.load(open(sys.argv[1]))

if __name__ == '__main__':
    inps = parse()

    #Get current directory
    currdir = os.getcwd()

    #Check if GIAnT dir exists. Create it if not.
    if os.path.isdir(inps['prepDir']):
        print("{0} directory already exists".format(inps['prepDir']))
    else:
        os.mkdir(inps['prepDir']) 

    pairs = []
    #####Get list of IFGS
    if inps['list'] in ['',None]:
        pairs = SS.getPairDirs(dirname=inps['insarDir'])
    else:
        pairs = SS.pairDirs_from_file(inps['list'], base=inps['insarDir'])

    #Create ifg.list
    ifglist = os.path.join(inps['prepDir'], 'ifg.list')
    xObj = None
    if (not os.path.exists(ifglist)) or inps['force']:
        fid = open(ifglist, 'w')
        for pair in pairs:
            dates=os.path.basename(pair).split('_')
            xmlFile = os.path.join(pair, 'insarProc.xml')
            xObj = OB.fromstring(open(xmlFile,'r').read())
            
            try:
                bTop = xObj.baseline.perp_baseline_top
                bBot = xObj.baseline.perp_baseline_bottom
                bPerp = 0.5*(bTop + bBot)
            except:
                print("Pair %s processed with old version of ISCE")
                print("Baseline not available in insarProc.xml")
                bPerp = 0.0

            fid.write('{0}   {1}   {2:5.4f}  CSK\n'.format(dates[0], dates[1], bPerp))



        fid.close()

    #####Create example.rsc
    if xObj is None:
        exampleXML = os.path.join(pairs[0],'insarProc.xml')

        fid = open(exampleXML,'r')
        xObj = OB.fromstring(fid.read())
        fid.close()

    width = int(xObj.runGeocode.outputs.GEO_WIDTH)
    length = int(xObj.runGeocode.outputs.GEO_LENGTH)
    rng = float(xObj.runFormSLC.master.outputs.STARTING_RANGE)
    ht = float(xObj.runFormSLC.master.inputs.SPACECRAFT_HEIGHT) 
    re = float(xObj.runFormSLC.master.inputs.PLANET_LOCAL_RADIUS)
    inc = getIncAngle(rng, ht, re)

    rdict = {}
    rdict['width'] = width
    rdict['length'] = length
    rdict['heading'] = float(xObj.runGeocode.inputs.PEG_HEADING) * 180.0 / np.pi    
    rdict['wvl'] = float(xObj.runGeocode.inputs.RADAR_WAVELENGTH)
    rdict['deltarg'] = 30.
    rdict['deltaaz'] = 30.
    rdict['utc'] = Seconds(str(xObj.master.frame.SENSING_MID).split( ' ')[-1])

    #####Get Lat / Lon information
    maxLat = float(xObj.runGeocode.outputs.MINIMUM_GEO_LATITUDE) #Bug in ISCE
    minLat = float(xObj.runGeocode.outputs.MAXIMUM_GEO_LATITUDE)
    minLon = float(xObj.runGeocode.outputs.MINIMUM_GEO_LONGITUDE)
    maxLon = float(xObj.runGeocode.outputs.MAXIMUM_GEO_LONGITUDE)

    rscfile = os.path.join(inps['prepDir'], 'example.rsc')
    if (not os.path.exists(rscfile)) or inps['force']:
        temp.templateSetup(rdict, source=inps['tempDir'], 
            target=inps['prepDir'], filename='example.rsc')

    
    #####Create lat.flt, lon.flt, hgt.flt, hgt.flt.rsc
    print('Setting up height and geometry files.')
#    DEMfile = os.path.join(pairs[0],'dem.crop')
#    Dfile = os.path.join(inps.prepDir, 'hgt.flt')

#    if (not os.path.exists(Dfile)) or inps.force:
#        hdata = np.memmap(DEMfile, dtype=np.int16,
#            shape=(length, width), mode='r')

#        fdata = np.memmap(Dfile, dtype=np.float32,
#                shape=(length,width), mode='w+')

#        for kk in xrange(length):
#            fdata[kk,:] = hdata[kk,:]

#        del hdata
#        del fdata



#    shutil.copyfile(DEMfile, os.path.join(inps.prepDir, 'hgt.flt'))

    lat = np.linspace(maxLat, minLat, num=rdict['length']).astype(np.float32)
    lats = np.lib.stride_tricks.as_strided(lat, 
        shape=(rdict['length'], rdict['width']), strides=(4,0))
    lats.tofile(os.path.join(inps['prepDir'], 'lat.flt'))
    del lat, lats

    lon = np.linspace(minLon, maxLon, num=rdict['width']).astype(np.float32)
    lons = np.lib.stride_tricks.as_strided(lon, 
        shape=(rdict['length'],rdict['width']), strides = (0,4))
    lons.tofile(os.path.join(inps['prepDir'], 'lon.flt'))
    del lon,lons

#    hgtrsc = os.path.join(inps.prepDir, 'hgt.flt.rsc')
#    rdict = {}
#    rdict['width'] = width
#    rdict['length'] = length
#    rdict['lat1'] = maxLat
#    rdict['lon1'] = minLon
#    rdict['lat2'] = maxLat
#    rdict['lon2'] = maxLon
#    rdict['lat3'] = minLat
#    rdict['lon3'] = maxLon
#    rdict['lat4'] = minLat
#    rdict['lon4'] = minLon
#    rdict['deltarg'] = 30.
#    rdict['deltaaz'] = 30.

#    if (not os.path.exists(hgtrsc)) or inps.force:
#        temp.templateSetup(rdict, source=inps['tempDir'],
#                target=inps.prepDir, filename='hgt.flt.rsc')



    #######Create userfn.py
    rdict = {}
    rdict['relpath'] = os.path.relpath(inps['insarDir'], inps['prepDir'])
    rdict['polyorder'] = 1
    ufnfile = os.path.join(inps['prepDir'], 'userfn.py')
    if (not os.path.exists(ufnfile)) or inps['force']:
        temp.templateSetup(rdict, source=inps['tempDir'],
            target=inps['prepDir'], filename='userfn.py')

    ##########Create prepxml.py
    rdict = {}
    rdict['width'] = int(xObj.runGeocode.outputs.GEO_WIDTH)
    rdict['length'] = int(xObj.runGeocode.outputs.GEO_LENGTH)
    rdict['cohth'] = 0.2
    rdict['nvalid'] = int(0.7 * len(pairs))
    latlon = np.loadtxt(inps['refName'])
    rdict['rx0'] = int(latlon[1]-10)
    rdict['rx1'] = int(latlon[1]+10)
    rdict['ry0'] = int(latlon[0]-10)
    rdict['ry1'] = int(latlon[0]+10)
    rdict['inc'] = inc
    rdict['filt'] = 0.15
    pxmlfile = os.path.join(inps['prepDir'], 'prepxml.py')
    if (not os.path.exists(pxmlfile)) or inps['force']:
        temp.templateSetup(rdict, source=inps['tempDir'],
            target=inps['prepDir'], filename='prepxml.py')
