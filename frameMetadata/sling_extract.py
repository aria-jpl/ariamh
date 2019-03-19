#!/usr/bin/env python
"""
Bootstrap the generation of a canonical product by downloading data
from the repository, creating the skeleton directory structure for
the product and leveraging the configured metadata extractor defined
for the product in datasets JSON config.
"""

import os, sys, re, json, shutil, requests, logging, traceback, argparse
from subprocess import check_output
import time

from hysds.recognize import Recognizer
import osaka.main

SCRIPT_RE = re.compile(r'script:(.*)$')

# all file types
ALL_TYPES = []

# zip types
ZIP_TYPE = [ "zip" ]
ALL_TYPES.extend(ZIP_TYPE)

# tar types
TAR_TYPE = [ "tbz2", "tgz", "bz2", "gz" ]
ALL_TYPES.extend(TAR_TYPE)


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

def verify(path, file_type):
    """Verify downloaded file is okay by checking that it can
       be unzipped/untarred."""

    test_dir = "./extract_test"
    if file_type in ZIP_TYPE:
        if not zipfile.is_zipfile(path):
            raise RuntimeError("%s is not a zipfile." % path)
        with zipfile.ZipFile(path, 'r') as f:
            f.extractall(test_dir)
        shutil.rmtree(test_dir, ignore_errors=True) 
    elif file_type in TAR_TYPE:
        if not tarfile.is_tarfile(path):
            raise RuntimeError("%s is not a tarfile." % path)
        with tarfile.open(path) as f:
            f.extractall(test_dir)
        shutil.rmtree(test_dir, ignore_errors=True) 
    else:
        raise NotImplementedError("Failed to verify %s is file type %s." % \
                                  (path, file_type))

def sling(download_url, repo_url, prod_name, file_type, prod_date, prod_met=None,
          oauth_url=None, force=False, force_extract=False):
    """Download file, push to repo and submit job for extraction."""

    # log force flags
    logging.info("force: %s; force_extract: %s" % (force, force_extract))

    # get localize_url
    if repo_url.startswith('dav'):
        localize_url = "http%s" % repo_url[3:]
    else: localize_url = repo_url

    # get filename
    path = os.path.basename(repo_url)

    is_here = False


    # download from source if not here or forced
    if not is_here or force:

        # download
        logging.info("Downloading %s to %s." % (download_url, path))
        try: osaka.main.get(download_url, path, params={ "oauth": oauth_url },measure=True,output="./pge_metrics.json")
        except Exception, e:
            tb = traceback.format_exc()
            logging.error("Failed to download %s to %s: %s" % (download_url,
                                                               path, tb))
            raise

        # verify downloaded file was not corrupted
        logging.info("Verifying %s is file type %s." % (path, file_type))
        try: verify(path, file_type)
        except Exception, e:
            tb = traceback.format_exc()
            logging.error("Failed to verify %s is file type %s: %s" % \
                          (path, file_type, tb))
            raise
        # Make a product here
        dataset_name = "incoming-" + prod_date + "-" + os.path.basename(path)
        proddir = os.path.join(".", dataset_name)
        os.makedirs(proddir)
        shutil.move(path, proddir)
        metadata = {
                       "download_url" : download_url,
                       "prod_name" : prod_name,
                       "prod_date" : prod_date,
                       "file": os.path.basename(localize_url),
                       "data_product_name" : os.path.basename(path),
                       "dataset" : "incoming",
                   }
            
        # Add metadata from context.json
        if prod_met is not None:
           prod_met = json.loads(prod_met)
           if prod_met:
             metadata.update(prod_met)
        
        # dump metadata
        with open(os.path.join(proddir, dataset_name + ".met.json"),"w") as f:
           json.dump(metadata,f)
           f.close()

        # get settings
        settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                 'settings.json')
        if not os.path.exists(settings_file):
            settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                 'settings.json.tmpl')
        settings = json.load(open(settings_file))

        # dump dataset
        with open(os.path.join(proddir, dataset_name + ".dataset.json"),"w") as f:
           dataset_json = { "version":settings["INCOMING_VERSION"] }
           if 'spatial_extent' in prod_met:
               dataset_json['location'] = prod_met['spatial_extent']
           json.dump(dataset_json, f)
           f.close()

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
        m = check_output([extractor, prod_path])
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
    run_extractor(dsets_file, prod_path, url, ctx)

def is_non_zero_file(fpath):  
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("slc_id", help="id of the localized file") 
    parser.add_argument("source", help="url source of the localized file")
    parser.add_argument("download_url", help="download_url of the localized file")
    parser.add_argument("file", help="localized product file")
    parser.add_argument("prod_name", help="product name to use for " +
                                          " canonical product directory")
    parser.add_argument("prod_date", help="product date to use for " +
                                      " canonical product directory")
    args = parser.parse_args()
    

    localize_url = None
    if args.source.lower()=="asf":
        vertex_url = "https://datapool.asf.alaska.edu/SLC/SA/{}.zip".format(args.slc_id)
        r = requests.head(vertex_url, allow_redirects=True)
        logging.info("Status Code from ASF : %s" %r.status_code)
        if r.status_code in (200, 403):
            localize_url = r.url
        else:
            raise RuntimeError("Status Code from ASF for SLC %s : %s" %(args.slc_id, r.status_code))
    else:
        localize_url = args.download_url
        
    try:
        filename, file_extension = os.path.splitext(args.file)
        logging.info("localize_url : %s \nfile : %s" %(localize_url, args.file))
       
        sling(localize_url, filename, args.prod_name, "zip", args.prod_date)
        #, prod_met=None, oauth_url=None, force=False, force_extract=False)

        '''
        try:
            logging.info("calling osaka")
            osaka.main.get(localize_url, args.file)
            logging.info("calling osaka successful")
        except:
            logging.info("calling osaka failed. sleeping ..")
            time.sleep(100)
            logging.info("calling osaka again")
            osaka.main.get(localize_url, args.file)
            logging.info("calling osaka successful")
         '''
        #Corrects input dataset to input file, if supplied input dataset 
        if os.path.isdir(args.file):
             shutil.move(os.path.join(args.file,args.file),"./tmp")
             shutil.rmtree(args.file)
             shutil.move("./tmp",args.file)

        #Check for Zero Sized File
        if not is_non_zero_file(args.file):
            raise Exception("File Not Found or Empty File : %s" %args.file)

        create_product(args.file, localize_url, args.prod_name, args.prod_date)
    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
