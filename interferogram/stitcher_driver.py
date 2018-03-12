from interferogram.stitcher_utils import main as main_st, get_mets, get_dates
import sys
import json
import os
def main(args):
    if args[0] == 'validate_ifg':
        '''
        Create a set of creteria to select ifg and get the metadata. The metadata will
        be the actual input to the validator, here just using helper function
        to create the metadata file, but it will come from facet view.
        NOTE: might be useful for some user to actually select the info below from 
        drop-down memu as input parameters, so have both options.
        NOTE: whatever is selected from facet needs to be binned into tracks and the 
              latitudeIndexMin and Max should be computed from aoi.
        NOTE: this stage not really insightful. Should focus more on timeseries 
        inputs:
            the below dictionary
        outputs:
            json file defined by "output_file" with the keys urls_list of dict containing the
            url of the product that fully cover the aio and have all swaths specified, and dates_incomplete
            with all the pair dates that are not completed. latter can be used for diagnostic
            
        '''
        inps = {
                "swaths":[1,2,3],
                "direction":"descending",
                "track":172,
                "platforms":["Sentinel-1A","Sentinel-1B"],
                "conf":"settings.conf",
                "latitudeIndexMin":160,
                "latitudeIndexMax":170,
                "sys_ver":"v1*",
                "tags":"track172",
                "output_file":"valid_ifg_out.json",
                "meta_file":"valid_meta_out.json"
                }
        mets = get_mets(inps)
        #dump the meta used for input
        json.dump(mets,open(inps['meta_file'],'w'), indent=2, sort_keys=True)
        #create input file for validate
        json.dump(inps,open('valid_ifg_in.json','w'), indent=2, sort_keys=True)
        main_st(('-a validate_ifg_met -i ' + 'valid_ifg_in.json').split())
    elif args[0] == 'validate_ts':
        '''
        As above except that the output file will return the list of the urls that complete 
        the spacial requiraments but also temporal, i.e. no gaps in time series.
        If there are gaps the json output_file contains a key "gaps" with the dates that are not filled
        '''
        inps = {
                "swaths":[1,2,3],
                "direction":"descending",
                "track":42,
                "platforms":["Sentinel-1A","Sentinel-1B"],
                "conf":"settings.conf",
                "latitudeIndexMin":375,
                "latitudeIndexMax":395,
                "sys_ver":"v1*",
                "tags":"-giangi",
                "output_file":"valid_ts_out.json",
                "meta_file":"valid_meta_ts_out.json",
                "min_repeat":12,
                "max_repeat":48,
                "only_best":False
                }
        mets = get_mets(inps)
        json.dump(mets,open(inps['meta_file'],'w'), indent=2, sort_keys=True)
        #create input file for validate
        json.dump(inps,open('valid_ts_in.json','w'), indent=2, sort_keys=True)
        main_st(('-a validate_ts_met -i ' +'valid_ts_in.json').split())
    elif args[0] == 'download_stitch':
        '''
        Based on the output of validate_ts_met localize the data and run stitcher.
        The results from the previous stage will be a set of ifgs that need to be stitch.
        the worker will creates and queue many pge like the one below
        '''
        #get the name of the output of the validate_ts stage and load the data
        inp_name = json.load(open('valid_ifg_in.json'))['output_file']
        inps = json.load(open(inp_name))
        jdict = {}
        #get the first element of the list as an example to donwload the data
        jdict['urls'] = inps['urls_list'][0]
        jdict['swaths'] = [1,2,3]
        jdict['products'] = ['filt_topophase.unw.conncomp.geo','filt_topophase.unw.geo','phsig.cor.geo']
        jdict['outname'] = 'filt_topophase.unw.geo'
        #get the first url of the swath 1 and from the name get the dates
        mdate,sdate = get_dates(jdict['urls']['1'][0])
        jdict['dirname'] = 'S1-IFG_STITCHED_' + mdate + '-' + sdate
        jdict['ifg_names'] = 'ifg_names.json'
        json.dump(jdict,open('donwload_in.json','w'), indent=2, sort_keys=True)
        #download the example we picked
        main_st(('-a download -i donwload_in.json').split())
        dirname = jdict['dirname']
        #previous stage prepared all the data, now stitch
        jdict_st = {}
        #here define any extra product that needs to be stiched. This info will be
        #entered when generating the job by the operator and propagated down to this
        #point into the context file
        jdict_st['extra_products'] = ['los.rdr.geo']
        jdict_st['direction'] = 'across'
        jdict_st['outname'] = 'filt_topophase.unw.geo'
        jdict_st['filenames'] = json.load(open(jdict['ifg_names']))
        jdict_st['']
        os.chdir(dirname)
        json.dump(jdict_st,open('ifg_stitch.json','w'), indent=2, sort_keys=True)
        main_st(('-a stitch -i ifg_stitch.json').split())
        os.chdir('../')
        
    else:
        print('Unrecogized option',args[0])
        
if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
