from builtins import str
from time_series.swath_stitcher import SwathStitcher as SS
import sys
import argparse
import numpy as np
from utils.contextUtils import toContext
import json
import shutil
from datetime import datetime as dt
import os
def parse(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--files', dest='files',type=str, nargs='+',
                    help='Filenames of the h5 timeseries')
    parser.add_argument('-o', '--output', dest='output',type=str,
                    help='Filename of the merged timeseries')
    return parser.parse_args(args)
'''
inputs in json file
{
"version":ver#the dataset version. mandatory
"label": extra info thar user might provide. optional 
"product_orig":[o1,o2,o3] #the names of the original products for provenance
"files":[f1,f2,f3] # path to the inputs. at least 2
"output":fname #name of the output product
"dataset_id":pname #name to be used to create the product dir
               and the json.met
"ts_type":ts_type # if it's generated from LS or NSBAS or other
"track_number":track_number# to display the metadata
}
The code expects the files to be already localized 
'''   
       
def main(fname):
    process = 'driver_swath_stitcher'
    try:
        inps = json.load(open(fname))
        ss = SS()
        if(len(inps['files']) < 2):
            print('Expecting at least two input files')
            raise Exception
        ss.load_ts(inps['files'])
        ss.create_output(inps['output'])
        ss._niter = inps['niter']
        ss.merge_datasets()
        try:
            os.mkdir(inps['dataset_id'])
        except Exception:
            pass
        met= {}
        dset= {}
        met['product_orig'] = inps['product_orig']
        met['ts_type'] = inps['ts_type']
        met['track_number'] = inps['track_number']
        minlat,maxlat,dlat,minlon,maxlon,dlon = ss.get_common_bbox()
        minlat = round(minlat,1)
        maxlat = round(maxlat,1)
        minlon = round(minlon,1)
        maxlon = round(maxlon,1)
        dset['location'] = {'type':'Polygon','coordinate':
                           [[minlon,maxlat],[maxlon,maxlat],
                            [maxlon,minlat],[minlon,minlat],
                            [minlon,maxlat]]}
        dset['starttime'] = dt.fromtimestamp(ss._fpo['time'][ss._dates_indx[0]][0]).isoformat()
        dset['endtime'] = dt.fromtimestamp(ss._fpo['time'][ss._dates_indx[0]][-1]).isoformat()
        dset['version'] = inps['version']
        if 'label' in inps:
            dset['label'] = inps['label']
        
        fp = open(os.path.join(inps['dataset_id'],inps['dataset_id'] + '.met.json'),'w')
        json.dump(met,fp,indent=4)
        fp.close()
        fp = open(os.path.join(inps['dataset_id'],inps['dataset_id'] + '.dataset.json'),'w')
        json.dump(dset,fp,indent=4)
        fp.close()
        shutil.move(inps['output'],inps['dataset_id'])
        
    except Exception as e:
            message = 'driver_swath_stitcher.py: run failed with exception ' + str(e)
            exit = 1
            toContext(process,exit,message)
            raise
   
if __name__ == '__main__':
    sys.exit(main(sys.argv[1]))
