#!/usr/bin/env python
"""
Bootstrap the generation of a canonical product by downloading data
from the repository, creating the skeleton directory structure for
the product and leveraging the configured metadata extractor defined
for the product in datasets JSON config.
"""

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
import hashlib
from datetime import datetime
import random
import time
#from utils.UrlUtils import UrlUtils

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
          ],
         "must_not":{"term": {"metadata.tags": "deprecated"}}
        }
      },
      "partial_fields" : {
            "partial" : {
                "exclude" : "city",
            }
        }
    }

    logging.info(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        logging.info("Failed to query %s:\n%s" % (es_url, r.text))
        logging.info("query: %s" % json.dumps(query, indent=2))
        logging.info("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    logging.info(result['hits']['total'])
    return result['hits']['hits']


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

    logging.info(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    logging.info("search_url : %s" %search_url)

    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        logging.info("Failed to query %s:\n%s" % (es_url, r.text))
        logging.info("query: %s" % json.dumps(query, indent=2))
        logging.info("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    logging.info(result['hits']['total'])
    return result


def check_slc_status(slc_id, index_suffix=None):

    result = get_dataset(slc_id, index_suffix)
    total = result['hits']['total']
    logging.info("check_slc_status : total : %s" %total)
    if total > 0:
        logging.info("check_slc_status : returning True")
        return True

    logging.info("check_slc_status : returning False")
    return False


def get_slc_checksum_md5_asf(slc_id):
    '''
    :param slc_id: slc_id taken from the metadata of the acquisition
    :return: string (md5 hash from ASF sci-hub) all lower case (ex. 8e15beebbbb3de0a7dbed50a39b6e41b)
    '''

    # sleeps random time between 15 and 60 seconds so we can further avoid too many requests to sci-hub
    sleep_time = random.randrange(15, 60)
    time.sleep(sleep_time)

    asf_geo_json_endpoint_template = "https://api.daac.asf.alaska.edu/services/search/param?granule_list={slc_id}&processingLevel=SLC&output=geojson"
    asf_geo_json_endpoint = asf_geo_json_endpoint_template.format(slc_id=slc_id)

    req = requests.get(asf_geo_json_endpoint, timeout=30)

    if req.status_code != 200:
        raise RuntimeError("API Request failed for md5 retrieval from ASF: ERROR CODE: {}".format(req.status_code))

    geojson = json.loads(req.text)
    if len(geojson["features"]) < 1:
        # {u'type': u'FeatureCollection', u'features': []} if SLC not found in ASF
        raise RuntimeError("SLC_ID {} not found in ASF: no available md5 checksum for SLC".format(slc_id))

    md5_hash = geojson["features"][0]["properties"]["md5sum"]  # md5 checksum is lower case
    return md5_hash


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
                    logging.info("{} matched '{}' for profile {}.".format(url, bucket_pattern, prof['profile']))
                    params['profile_name'] = prof['profile']
                    break
                
    return params


def update_context_file(localize_url, file_name, prod_name, prod_date, download_url):
    logging.info("update_context_file :%s,  %s" %(localize_url, file_name))
    ctx_file = "_context.json"
    localized_url_array = []
    url_dict = {}
    url_dict["local_path"] = file_name
    url_dict["url"]=localize_url

    localized_url_array.append(url_dict)
    with open(ctx_file) as f:
        ctx = json.load(f)
    ctx["localize_urls"] = localized_url_array
    ctx["prod_name"] = prod_name
    ctx["file"] = file_name
    ctx["download_url"] = download_url
    ctx["prod_date"] = prod_date

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
            logging.info("cache hit for {} at {}".format(url, cache_dir))
        else:
            logging.info("cache miss for {}".format(url))
            try:
                osaka.main.get(url, cache_dir, params=params)
            except Exception, e:
                shutil.rmtree(cache_dir)
                tb = traceback.format_exc()
                raise(RuntimeError("Failed to download %s to cache %s: %s\n%s" % \
                    (url, cache_dir, str(e), tb)))
            with atomic_write(signal_file, overwrite=True) as f:
                f.write("%sZ\n" % datetime.utcnow().isoformat())
        for i in os.listdir(cache_dir):
            if i == '.localized':
                continue
            cached_obj = os.path.join(cache_dir, i)
            if os.path.isdir(cached_obj):
                dst = os.path.join(path, i) if os.path.isdir(path) else path
                try:
                    os.symlink(cached_obj, dst)
                except:
                    logger.error("Failed to soft link {} to {}".format(cached_obj, dst))
                    raise("Failed to soft link {} to {}".format(cached_obj, dst))
            else:
                try:
                    os.symlink(cached_obj, path)
                except:
                    logger.error("Failed to soft link {} to {}".format(cached_obj, path))
                    raise("Failed to soft link {} to {}".format(cached_obj, dst))
    else:
        return osaka.main.get(url, path, params=params)


def localize_file(url, path, cache):
    """Localize urls for job inputs. Track metrics."""

    # get job info
    job_dir = os.getcwd() #job['job_info']['job_dir']

    # localize urls
    if path is None:
        path = '%s/' % job_dir
    else:
        if path.startswith('/'):
            pass
        else:
            path = os.path.join(job_dir, path)
    if os.path.isdir(path) or path.endswith('/'):
        path = os.path.join(path, os.path.basename(url))
    dir_path = os.path.dirname(path)
    logging.info(dir_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    loc_t1 = datetime.utcnow()
    try:
        download_file(url, path, cache=cache)
    except Exception, e:
        tb = traceback.format_exc()
        raise(RuntimeError("Failed to download %s: %s\n%s" % (url, str(e), tb)))
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


def get_md5_from_localized_file(file_name):
    '''
    :param file_name: file path to the local SLC file after download
    :return: string, ex. 8e15beebbbb3de0a7dbed50a39b6e41b ALL LOWER CASE
    '''
    hash_md5 = hashlib.md5()
    with open(file_name, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_log_err(log_file):
    err_msg = None
    try:
        with open(log_file, 'r') as f:
            last = None
            for last in (line for line in f if line.rstrip('\n')):
                pass
            if last:
                err_msg = last
    except Exception as err:
        logging.info("Error reading %s : %s" %(log_file, str(err)))

    return err_msg      


def run_extractor(dsets_file, prod_path, url, ctx, md5_hash):
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
    r = Recognizer(dsets_file, prod_path, os.path.basename(prod_path), settings["EXTRACT_VERSION"])
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

    with open(os.path.join(prod_path, '%s.zip.md5' % os.path.basename(prod_path)), 'w') as md5_file:
        md5_file.write(md5_hash)  # writing md5 hash into zip file if it passes

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
            err_msg = e.message
            root_dir = os.getcwd()
            logging.info("root_dir with getcwd() : %s" %root_dir)
            if not root_dir.endswith("Z"):
                root_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))

            logging.info("root_dir final : %s" %root_dir)
            prov_log = os.path.join(root_dir, 'create_prov_es.log')
            split_log  = os.path.join(root_dir, 'split_swath_products.log')
            logging.info("%s\n%s" %(prov_log, split_log))
            if os.path.isfile(prov_log):
                prov_err = get_log_err(prov_log)
                if prov_err:
                    err_msg = prov_err
            elif os.path.isfile(split_log):
                split_err = get_log_err(split_log)
                if split_err:
                    err_msg=split_err
            else:
                logging.info("%s file NOT Found" %split_log) 
            raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, err_msg))

        if os.path.exists(metadata_file):
            with open(metadata_file) as f:
                metadata.update(json.load(f))

    # set data_product_name
    metadata['data_product_name'] = objectid

    # set download url from context
    metadata['download_url'] = url

    # add md5 hash in metadata
    metadata['md5_hash'] = md5_hash

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


def create_product(file, url, prod_name, prod_date, md5_hash):
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
        os.makedirs(prod_path, 0775)
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

    run_extractor(dsets_file, prod_path, url, ctx, md5_hash)


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

    time.sleep(5)
    #Recheck as this method sometime does not work    
    if check_slc_status(args.slc_id.strip()):
        logging.info("Existing as we FOUND slc id : %s in ES query" %args.slc_id)
        exit(0)

    acq_datas = get_acquisition_data_from_slc(args.slc_id)
    if len(acq_datas)<1:
        raise RuntimeError("No Non-Deprecated Acquisition Found for SLC: {}".format(args.slc_id))

    acq_data = acq_datas[0]
    if len(acq_datas)>1:
        for x in range(len(acq_datas)):
            acq_data = acq_datas[x]
            logging.info("Processing : {}".format(acq_data['_id']))
            if 'esa_scihub' in acq_data['_id']:
                break

    logging.info("Acquisition : {}".format(acq_data['_id']))
    acq_data = acq_data['fields']['partial'][0]
    download_url = acq_data['metadata']['download_url']
    archive_filename = acq_data['metadata']['archive_filename']
    logging.info("download_url : %s" %download_url)
    logging.info("archive_filename : %s" %archive_filename)
    logging.info("acq_data['metadata']['id'] : %s" %acq_data['metadata']['id'])

    # get md5 checksum from ASF sci-hub
    asf_md5_hash = get_slc_checksum_md5_asf(args.slc_id)

    source = "asf"
    localize_url = None
    if source.lower() == "asf":
        localize_url = "https://datapool.asf.alaska.edu/SLC/SA/{}.zip".format(args.slc_id)
    else:
        localize_url = download_url
        
    try:
        filename, file_extension = os.path.splitext(archive_filename)
        logging.info("localize_url : %s \nfile : %s" %(localize_url, archive_filename))
       
        localize_file(localize_url, archive_filename, False)

        # update context.json with localize file info as it is used later
        update_context_file(localize_url, archive_filename, args.slc_id, prod_date, download_url)

        # getting the checksum value of the localized file
        os.path.abspath(archive_filename)
        #slc_file_path = os.path.join(os.path.abspath(args.slc_id), archive_filename)
        slc_file_path = os.path.join(os.getcwd(), archive_filename)
        localized_md5_checksum = get_md5_from_localized_file(slc_file_path)

        # comparing localized md5 hash with asf's md5 hash
        if localized_md5_checksum != asf_md5_hash:
            raise RuntimeError("Checksums DO NOT match SLC id {} : SLC checksum {}. local checksum {}".format(args.slc_id, asf_md5_hash, localized_md5_checksum))

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

        create_product(archive_filename, localize_url, args.slc_id, prod_date, asf_md5_hash)
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
