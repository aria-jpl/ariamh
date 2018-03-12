#!/usr/bin/env python
'''
Auditor tool that runs the enumerator and checks against the
existing interferograms to ensure that they exist

@author mstarch
'''
import sys
import json
import logging
import traceback
import enumerate_topsapp_cfgs
from hysds_commons.request_utils import post_scrolled_json_responses
from utils.UrlUtils import UrlUtils as UU

LOG_FORMAT = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
LOGGER = logging.getLogger('audit_interferograms')
class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

LOGGER.setLevel(logging.INFO)
LOGGER.addFilter(LogFilter())


def get_audit_existence_query(configs, version):
    '''
    Build query from configurations
    @param cfgs: configurations
    @param version: version of interferogram
    '''
    query = {
        "query": {
            "bool": {
                "should" : []
            }
        },
        "fields":[]
    }
    for ifg in configs[3]:
        query["query"]["bool"]["should"].append({"term": {"id.raw": ifg+"-"+version}})
    return query

def audit(configs, es_url, es_index, version):
    '''
    Audit cfgs and return only the ones that do not exist
    @param configs: job configurationsa
    @param es_url: elastic search url
    @param es_index: elastic search index
    @param version: version of interferogram to check
    '''
    #Query here
    start_url = "{}/{}/_search".format(es_url, es_index)
    scroll_url = "{}/_search".format(es_url)
    ret = ([],[],[],[],[],[],[],[],[],[])
    def chunk(items, size):
        '''
        Chunks the lists inside the supplied tuple into
        evenly spaces sets of size items.
        Note: to be memory efficient this destroys "items"
        @param items: items tuple to chunk and eat
        @param size: size to chunk into
        '''
        chunks = [[] for i in items]
        while len(items[0]) > 0:
            for i in xrange(0, len(chunks)):
                chunks[i] = items[i][0: size]
                del items[i][0: size]
            yield tuple(chunks)
    #Go through the configs 300 at a time, filtering each set
    #Note: this destroys configs
    for subcfgs in chunk(configs, 300): 
        query = json.dumps(get_audit_existence_query(subcfgs, version))
        #LOGGER.info("Running existence query: %s" % query)
        resps = post_scrolled_json_responses(start_url, scroll_url, True, data=query, logger=LOGGER)
        #LOGGER.info("Responses: %s" % resps)
        interferograms = {result.get("_id", "no-id").replace("-"+version,"") for result in resps}
        #LOGGER.info("Existing interferograms: %s" % interferograms)
        LOGGER.info("Existing: %d interferograms" % len(interferograms))
        LOGGER.info("Enumerated: %d interferograms" % len(subcfgs[0]))
        
        cnt = 0
        for i in range(len(subcfgs[0])):
            if subcfgs[3][i] in interferograms:
                continue
            cnt = cnt + 1
            for j in range(len(subcfgs)):
                ret[j].append(subcfgs[j][i])
        LOGGER.info("Filtered to %d configs:" % cnt)
        #print("Filtered to %d configs:" % len(ret[0]))
        #for i in range(len(ret[0])):
        #    #Filter out existing interferograms
        #    print("#" * 80)
        #    print("project: %s" % ret[0][i])
        #    print("stitched: %s" % ret[1][i])
        #    print("auto_bbox: %s" % ret[2][i])
        #    print("ifg_id: %s" % ret[3][i])
        #    print("master_zip_url: %s" % ret[4][i])
        #    print("master_orbit_url: %s" % ret[5][i])
        #    print("slave_zip_url: %s" % ret[6][i])
        #    print("slave_orbit_url: %s" % ret[7][i])
        #    print("swath_nums: %s" % ret[8][i])
        #    print("bbox: %s" % ret[9][i])
    return ret
def get_audit_input_query(starttime, endtime, coordinates):
    '''
    Get the query for the audit inteferograms
    @param starttime: start time for query
    @param endtime: endtime for the query
    @param coordinates: coordinates for the query
    '''
    #Setup for the query
    return {
        "filtered": {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "dataset.raw": "S1-IW_SLC"
                            }
                        },
                        {
                            "range": {
                                "starttime": {
                                    "from": starttime,
                                    "to": endtime
                                }
                            }
                        }
                    ]
                }
            },
            "filter": {
                "geo_shape": {
                    "location": {
                        "shape": coordinates
                    }
                }
            }
        }
    }
def run_auditor(context, dataset="ifg"):
    '''
    Route auditor for dataset type
    '''
    try:
        #Read existing
        with open(context, "r") as fh1:
            context = json.load(fh1)
        try:
            coordinates = json.loads(context["audit_coordinates"])
        except TypeError as ex:
            coordinates = context["audit_coordinates"]
        context["query"] = {"query": get_audit_input_query(context["audit_starttime"],
                                                 context["audit_endtime"],
                                                 coordinates)}
        #Write out new context
        enum_context = "enum_context.json"
        with open(enum_context, "w") as fh2:
            json.dump(context, fh2)
        #Call the pair-gen code with new context
        LOGGER.info("Enumerating IFGs" )
        if dataset == "ifg":
            cfgs = enumerate_topsapp_cfgs.get_topsapp_cfgs("enum_context.json")
        elif dataset == "slcp":
            cfgs = enumerate_topsapp_cfgs.get_topsapp_cfgs_rsp("enum_context.json")
        else:
            raise RuntimeError("Unknown dataset type for auditor: %s" % dataset)

        # query docs
        url_util = UU()
        LOGGER.info("rest_url: %s" % url_util.rest_url)
        LOGGER.info("grq_index_prefix: %s" % url_util.grq_index_prefix)
        LOGGER.info("version: %s" % url_util.version)
        # get normalized rest url
        rest_url = url_util.rest_url[:-1] if url_util.rest_url.endswith('/') else url_util.rest_url
        return audit(cfgs, rest_url, url_util.grq_index_prefix, url_util.version)
    except Exception as ex:
        with open('_alt_error.txt', 'w') as fh1:
            fh1.write("{}\n".format(ex))
        with open('_alt_traceback.txt', 'w') as fh2:
            fh2.write("{}\n".format(traceback.format_exc()))
        LOGGER.error("Exception of type %s occured with message %s" % (type(ex), ex))
        LOGGER.error("Traceback:\n%s" % traceback.format_exc())
        raise
def main(context):
    '''
    Main script for IFG auditor
    '''
    return run_auditor(context)
def main_rsp(context):
    '''
    Main script for SLCP auditor
    '''
    return run_auditor(context, dataset="slcp")
if __name__ == "__main__":
    main("_context.json")
    sys.exit(0)
