#!/usr/bin/env python
"""
Bootstrap the generation of a canonical product by downloading data
from the repository, creating the skeleton directory structure for
the product and leveraging the configured metadata extractor defined
for the product in datasets JSON config.
"""

from builtins import str
import os, sys, re, hashlib, json, shutil, requests, logging, traceback, argparse
from subprocess import check_output, CalledProcessError
import time
import tarfile, zipfile
from hysds.recognize import Recognizer
import osaka.main
from atomicwrites import atomic_write
import hysds
from hysds.log_utils import logger, log_prov_es
from hysds.celery import app
from datetime import datetime

SCRIPT_RE = re.compile(r'script:(.*)$')

# all file types
ALL_TYPES = []

# zip types
ZIP_TYPE = [ "zip" ]
ALL_TYPES.extend(ZIP_TYPE)

# tar types
TAR_TYPE = [ "tbz2", "tgz", "bz2", "gz" ]
ALL_TYPES.extend(TAR_TYPE)

CONF_FILE="/home/ops/ariamh/conf/settings.conf"

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

def get_download_params(url):
    """Set osaka download params."""

    params = {}

    # set profile
    for prof in app.conf.get('BUCKET_PROFILES', []):
        if 'profile_name' in params: break
        if prof.get('bucket_patterns', None) is None:
            params['profile_name'] = prof['profile']
            break
        else:
            if isinstance(prof['bucket_patterns'], list):
                bucket_patterns = prof['bucket_patterns']
            else: bucket_patterns = [ prof['bucket_patterns'] ]
            for bucket_pattern in prof['bucket_patterns']:
                regex = re.compile(bucket_pattern)
                match = regex.search(url)
                if match:
                    logger.info("{} matched '{}' for profile {}.".format(url, bucket_pattern, prof['profile']))
                    params['profile_name'] = prof['profile']
                    break
                
    return params

def update_context_file(localize_url, file_name):
    print("update_context_file :%s,  %s" %(localize_url, file_name))
    ctx_file = "_context.json"
    localized_url_array = []
    url_dict = {}
    url_dict["local_path"] = file_name
    url_dict["url"]=localize_url

    localized_url_array.append(url_dict)
    with open(ctx_file) as f:
        ctx = json.load(f)
    ctx["localize_urls"] = localized_url_array

    with open(ctx_file, 'w') as f:
        json.dump(ctx, f, indent=2, sort_keys=True)
 
def download_file(url, path, cache=False):
    """Download file/dir for input."""

    params = get_download_params(url)
    if cache:
        url_hash = hashlib.md5(url).hexdigest()
        hash_dir = os.path.join(app.conf.ROOT_WORK_DIR, 'cache', *url_hash[0:4])
        cache_dir = os.path.join(hash_dir, url_hash)
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        signal_file = os.path.join(cache_dir, '.localized')
        if os.path.exists(signal_file):
            logger.info("cache hit for {} at {}".format(url, cache_dir))
        else:
            logger.info("cache miss for {}".format(url))
            try: osaka.main.get(url, cache_dir, params=params)
            except Exception as e:
                shutil.rmtree(cache_dir)
                tb = traceback.format_exc()
                raise RuntimeError
            with atomic_write(signal_file, overwrite=True) as f:
                f.write("%sZ\n" % datetime.utcnow().isoformat())
        for i in os.listdir(cache_dir):
            if i == '.localized': continue
            cached_obj = os.path.join(cache_dir, i)
            if os.path.isdir(cached_obj):
                dst = os.path.join(path, i) if os.path.isdir(path) else path
                try: os.symlink(cached_obj, dst)
                except:
                    logger.error("Failed to soft link {} to {}".format(cached_obj, dst))
                    raise
            else:
                try: os.symlink(cached_obj, path)
                except:
                    logger.error("Failed to soft link {} to {}".format(cached_obj, path))
                    raise
    else: return osaka.main.get(url, path, params=params)


def localize_file(url, path, cache):
    """Localize urls for job inputs. Track metrics."""


def getConf():
    uu ={}
    with open(CONF_FILE, 'r') as fp:
        allL = fp.readlines()
        dc = {}
        for line in allL:
            ls = line.split('=')
            if(len(ls) == 2):                
                dc[ls[0]] = ls[1]
        fp.close()
        try:
            uu['rest_url'] = dc['GRQ_URL'].strip()
        except:
            uu['rest_url'] = None
            pass
        try:
            uu['dav_url'] = dc['ARIA_DAV_URL'].strip()
        except:
            uu['dav_url']=None
            pass
        try:
            uu['grq_index_prefix'] = dc['GRQ_INDEX_PREFIX'].strip()
        except:
            pass
        try:
            uu['datasets_cfg'] = dc['DATASETS_CONFIG'].strip()
        except:
            pass
    return uu


def get_acquisition_data_from_slc(slc_id):
    uu = getConf()
    es_url = uu['rest_url']
    es_index = "grq_*_*acquisition*"
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "metadata.identifier.raw": slc_id 
              }
            }
          ]
        }
      },
      "partial_fields" : {
            "partial" : {
                "exclude" : "city",
            }
        }
    }

    logger.info(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        logger.info("Failed to query %s:\n%s" % (es_url, r.text))
        logger.info("query: %s" % json.dumps(query, indent=2))
        logger.info("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    logger.info(result['hits']['total'])
    return result['hits']['hits'][0]


def get_dataset(id, es_index_data=None):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    uu = getConf()
    es_url = uu['rest_url']

    #es_index = "grq_*_{}".format(index_suffix.lower())
    es_index = "grq"
    if es_index_data:
        es_index = es_index_data

    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } }
                ]
            }
        },
        "fields": []
    }

    logger.info(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    logger.info("search_url : %s" %search_url)

    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        logger.info("Failed to query %s:\n%s" % (es_url, r.text))
        logger.info("query: %s" % json.dumps(query, indent=2))
        logger.info("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    logger.info(result['hits']['total'])
    return result

def check_slc_status(slc_id, index_suffix=None):

    result = get_dataset(slc_id, index_suffix)
    total = result['hits']['total']
    logger.info("check_slc_status : total : %s" %total)
    if total > 0:
        logger.info("check_slc_status : returning True")
        return True

    logger.info("check_slc_status : returning False")
    return False


def get_download_params(url):
    """Set osaka download params."""

    params = {}

    # set profile
    for prof in app.conf.get('BUCKET_PROFILES', []):
        if 'profile_name' in params: break
        if prof.get('bucket_patterns', None) is None:
            params['profile_name'] = prof['profile']
            break
        else:
            if isinstance(prof['bucket_patterns'], list):
                bucket_patterns = prof['bucket_patterns']
            else: bucket_patterns = [ prof['bucket_patterns'] ]
            for bucket_pattern in prof['bucket_patterns']:
                regex = re.compile(bucket_pattern)
                match = regex.search(url)
                if match:
                    logger.info("{} matched '{}' for profile {}.".format(url, bucket_pattern, prof['profile']))
                    params['profile_name'] = prof['profile']
                    break
                
    return params

def update_context_file(localize_url, file_name):
    logger.info("update_context_file :%s,  %s" %(localize_url, file_name))
    ctx_file = "_context.json"
    localized_url_array = []
    url_dict = {}
    url_dict["local_path"] = file_name
    url_dict["url"]=localize_url

    localized_url_array.append(url_dict)
    with open(ctx_file) as f:
        ctx = json.load(f)
    ctx["localize_urls"] = localized_url_array

    with open(ctx_file, 'w') as f:
        json.dump(ctx, f, indent=2, sort_keys=True)
 
def download_file(url, path, cache=False):
    """Download file/dir for input."""

    params = get_download_params(url)
    if cache:
        url_hash = hashlib.md5(url).hexdigest()
        hash_dir = os.path.join(app.conf.ROOT_WORK_DIR, 'cache', *url_hash[0:4])
        cache_dir = os.path.join(hash_dir, url_hash)
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        signal_file = os.path.join(cache_dir, '.localized')
        if os.path.exists(signal_file):
            logger.info("cache hit for {} at {}".format(url, cache_dir))
        else:
            logger.info("cache miss for {}".format(url))
            try: osaka.main.get(url, cache_dir, params=params)
            except Exception as e:
                shutil.rmtree(cache_dir)
                tb = traceback.format_exc()
                raise RuntimeError
            with atomic_write(signal_file, overwrite=True) as f:
                f.write("%sZ\n" % datetime.utcnow().isoformat())
        for i in os.listdir(cache_dir):
            if i == '.localized': continue
            cached_obj = os.path.join(cache_dir, i)
            if os.path.isdir(cached_obj):
                dst = os.path.join(path, i) if os.path.isdir(path) else path
                try: os.symlink(cached_obj, dst)
                except:
                    logger.error("Failed to soft link {} to {}".format(cached_obj, dst))
                    raise
            else:
                try: os.symlink(cached_obj, path)
                except:
                    logger.error("Failed to soft link {} to {}".format(cached_obj, path))
                    raise
    else: return osaka.main.get(url, path, params=params)


def localize_file(url, path, cache):
    """Localize urls for job inputs. Track metrics."""

    # get job info
    job_dir = os.getcwd() #job['job_info']['job_dir']

    # localize urls
    if path is None: path = '%s/' % job_dir
    else:
        if path.startswith('/'): pass
        else: path = os.path.join(job_dir, path)
    if os.path.isdir(path) or path.endswith('/'):
        path = os.path.join(path, os.path.basename(url))
    dir_path = os.path.dirname(path)
    logger.info(dir_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    loc_t1 = datetime.utcnow()
    try: download_file(url, path, cache=cache)
    except Exception as e:
        tb = traceback.format_exc()
        raise RuntimeError
    loc_t2 = datetime.utcnow()
    loc_dur = (loc_t2 - loc_t1).total_seconds()
    #path_disk_usage = get_disk_usage(path)
    '''
    job['job_info']['metrics']['inputs_localized'].append({
        'url': url,
        'path': path,
        'disk_usage': path_disk_usage,
        'time_start': loc_t1.isoformat() + 'Z',
        'time_end': loc_t2.isoformat() + 'Z',
        'duration': loc_dur,
        'transfer_rate': path_disk_usage/loc_dur
        })
    '''
    # signal run_job() to continue
    return True


def run_extractor(dsets_file, prod_path, url, ctx):
    """Run extractor configured in datasets JSON config."""

    logging.info("datasets: %s" % dsets_file)
    logging.info("prod_path: %s" % prod_path)
    # get settings
    settings = {}
    try:
        settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.json')
        settings = json.load(open(settings_file))
    except:

        settings['DATASETS_CFG'] = "/home/ops/verdi/etc/datasets.json"
        settings["INCOMING_VERSION"] = "v0.1"
        settings["EXTRACT_VERSION"]= "v0.1"
        settings["ACQ_TO_DSET_MAP"] = {"acquisition-S1-IW_SLC": "S1-IW_SLC"}

    # recognize
    r = Recognizer(dsets_file, prod_path,os.path.basename(prod_path),settings["EXTRACT_VERSION"])
    objectid = r.getId()

    # get extractor
    extractor = r.getMetadataExtractor()
    if extractor is not None:
        match = SCRIPT_RE.search(extractor)
        if match: extractor = match.group(1)
    logging.info("Configured metadata extractor: %s" % extractor)

    # metadata file
    metadata_file = os.path.join(prod_path, '%s.met.json' % \
                                 os.path.basename(prod_path))
    dataset_file = os.path.join(prod_path, '%s.dataset.json' % \
                                 os.path.basename(prod_path))

    # load metadata
    metadata = {}
    if os.path.exists(metadata_file):
        with open(metadata_file) as f:
            metadata = json.load(f)

    m = {}
    # run extractor
    if extractor is None:
        logging.info("No metadata extraction configured.")
    else:
        logging.info("Running metadata extractor %s on %s" % \
                    (extractor, prod_path))
        try:
            m = check_output([extractor, prod_path])
        except CalledProcessError as e:
            raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))

        if os.path.exists(metadata_file):
            with open(metadata_file) as f:
                metadata.update(json.load(f))

    # set data_product_name
    metadata['data_product_name'] = objectid 

    # set download url from context
    metadata['download_url'] = url

    # write it out to file
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    logging.info("Wrote metadata to %s" % metadata_file)

    # Build datasets and add in "optional" fields, if not already created by extractor
    if not os.path.exists(dataset_file):
        datasets = {"version":settings["EXTRACT_VERSION"]}
        for key in ["location","starttime","endtime","label"]:
            if key in m:
                datasets[key] = m[key]
        # write it out to file
        with open(dataset_file, 'w') as f:
            json.dump(datasets, f, indent=2)
        logging.info("Wrote dataset to %s" % dataset_file)

def create_product(file, url, prod_name, prod_date):
    """Create skeleton directory structure for product and run configured
       metadata extractor."""

    # get settings

    settings = {}
    try:
        settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.json')
        settings = json.load(open(settings_file))
    except:

        settings['DATASETS_CFG'] = "/home/ops/verdi/etc/datasets.json"
        settings["INCOMING_VERSION"] = "v0.1"
        settings["EXTRACT_VERSION"]= "v0.1"
        settings["ACQ_TO_DSET_MAP"] = {"acquisition-S1-IW_SLC": "S1-IW_SLC"}

    # create product directory and move product file in it
    prod_path = os.path.abspath(prod_name)
    if not os.path.exists(prod_path):
        os.makedirs(prod_path, 0o775)
    shutil.move(file, os.path.join(prod_path, file))

    # copy _context.json if it exists
    ctx = {}
    ctx_file = "_context.json"
    prod_ctx_file = "%s_%s.context.json" % (prod_name, prod_date)
    if os.path.exists(ctx_file):
        shutil.copy(ctx_file, os.path.join(prod_path, prod_ctx_file))
        with open(ctx_file) as f:
            ctx = json.load(f)

    # extract metadata
    dsets_file = settings['DATASETS_CFG']
    if os.path.exists("./datasets.json"):
        dsets_file = "./datasets.json"
    run_extractor(dsets_file, prod_path, url, ctx)

def is_non_zero_file(fpath):  
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("slc_id", help="id of the localized file") 
    args = parser.parse_args()
    prod_date = time.strftime('%Y-%m-%d' )    
 
    if check_slc_status(args.slc_id.strip()):
        logging.info("Existing as we FOUND slc id : %s in ES query" %args.slc_id)
        exit(0)

    time.sleep( 5 )
    #Recheck as this method sometime does not work    
    if check_slc_status(args.slc_id.strip()):
        logging.info("Existing as we FOUND slc id : %s in ES query" %args.slc_id)
        exit(0)

    acq_data = get_acquisition_data_from_slc(args.slc_id)['fields']['partial'][0]
    download_url = acq_data['metadata']['download_url']
    archive_filename = acq_data['metadata']['archive_filename']
    logging.info("download_url : %s" %download_url)
    logging.info("archive_filename : %s" %archive_filename)

    source = "asf"
    localize_url = None
    if source.lower()=="asf":
        vertex_url = "https://datapool.asf.alaska.edu/SLC/SA/{}.zip".format(args.slc_id)
        r = requests.head(vertex_url, allow_redirects=True)
        logging.info("Status Code from ASF : %s" %r.status_code)
        if r.status_code in (200, 403):
            localize_url = vertex_url
        else:
            raise RuntimeError("Status Code from ASF for SLC %s : %s" %(args.slc_id, r.status_code))
    else:
        localize_url = download_url
        
    try:
        filename, file_extension = os.path.splitext(archive_filename)
        logging.info("localize_url : %s \nfile : %s" %(localize_url, archive_filename))
       
        localize_file(localize_url, archive_filename, False)

        #update _context.json with localize file info as it is used later
        update_context_file(localize_url, archive_filename)


        '''
        try:
            logging.info("calling osaka")
            osaka.main.get(localize_url, archive_filename)
            logging.info("calling osaka successful")
        except:
            logging.info("calling osaka failed. sleeping ..")
            time.sleep(100)
            logging.info("calling osaka again")
            osaka.main.get(localize_url, archive_filename)
            logging.info("calling osaka successful")
         '''
        #Corrects input dataset to input file, if supplied input dataset 
        if os.path.isdir(archive_filename):
             shutil.move(os.path.join(archive_filename,archive_filename),"./tmp")
             shutil.rmtree(archive_filename)
             shutil.move("./tmp",archive_filename)

        #Check for Zero Sized File
        if not is_non_zero_file(archive_filename):
            raise Exception("File Not Found or Empty File : %s" %archive_filename)

        create_product(archive_filename, localize_url, args.slc_id, prod_date)
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
