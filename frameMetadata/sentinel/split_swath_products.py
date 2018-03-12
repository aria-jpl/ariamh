#!/usr/bin/env python
import os, sys, re, json, shutil, zipfile, argparse
from glob import glob

import create_met_json
import create_dataset_json


MISSION_RE = re.compile(r'^S1(\w)_')


def browse(extracted, safe_dir, productType):
    """Create browse images."""

    browse_img = extracted+".browse.png"
    small_browse_img = extracted+".browse_small.png"
    if productType == "slc":
        img = os.path.join(safe_dir, "preview", "quick-look.png")
        os.system("cp -f %s %s" % (img, browse_img))
        os.system("convert -resize 250x250 %s %s" % (browse_img, small_browse_img))


def harvest(extracted, safe_dir, productType):
    """Harvest the metadata for this product."""

    metf = extracted+".met.json"
    dsf = extracted+".dataset.json"
    mis_char = MISSION_RE.search(extracted).group(1)
    if productType == "slc" or productType == "raw":
        fn = "%s/manifest.safe" % safe_dir
        create_met_json.create_met_json(fn,metf,mis_char)
        create_dataset_json.create_dataset_json(extracted,metf,dsf)
    else:
        #Write JSON for this product
        metadata={"productname":extracted}
        with open(metf,"w") as f:
            f.write(json.dumps(metadata))
    return metf


def extract(zip_file):
    """Extract the zipfile."""

    with zipfile.ZipFile(zip_file, 'r') as zf:
        zf.extractall()
    prod = zip_file.replace(".zip", "")
    safe_dir = "%s.SAFE" % prod
    return prod, safe_dir


def split_swaths(extracted, safe_dir, job_dir):
    """Create separate products for each swath."""

    # create swath product
    print("extracted: %s" % extracted)
    print("safe_dir: %s" % safe_dir)
    for tiff_file in  glob("%s/measurement/*.tiff" % safe_dir):
        print("tiff_file: %s" % tiff_file)
        id = os.path.splitext(os.path.basename(tiff_file))[0]
        prod_dir = os.path.join(job_dir, "swaths", id) 
        print("prod_dir: %s" % prod_dir)
        if not os.path.isdir(prod_dir):
            os.makedirs(prod_dir, 0755)

        # get annotation
        ann_file = "%s/annotation/%s.xml" % (safe_dir, id)
        if not os.path.isfile(ann_file):
            raise RuntimeError("Failed to find annotation file %s." % ann_file)
        print("ann_file: %s" % ann_file)
        shutil.copy(ann_file, prod_dir)

        # copy browse image
        browse = "%s/browse.png" % prod_dir
        shutil.copy("%s/preview/quick-look.png" % safe_dir, browse)
        browse_small = "%s/browse_small.png" % prod_dir
        os.system("convert -resize 250x250 %s %s" % (browse, browse_small))


def parser():
    """Construct a parser to parse arguments."""

    parse = argparse.ArgumentParser(description="Split S1 granule into separate swath products.")
    parse.add_argument("zip_file", help="Zip file localized by HySDS")
    parse.add_argument("job_dir", help="job directory")
    return parse


if __name__ == "__main__":
    args = parser().parse_args()
    if re.search(r'S1\w_IW_SLC', args.zip_file): typ = 'slc'
    elif re.search(r'S1\w_IW_RAW', args.zip_file): typ = 'raw'
    else: raise RuntimeError("Unknown type: %s" % args.zip_file)
    extracted, safe_dir = extract(args.zip_file)
    split_swaths(extracted, safe_dir, args.job_dir)
    harvest(extracted, safe_dir, typ)
    browse(extracted, safe_dir, typ)
    os.system("rm -rf %s" % safe_dir)
