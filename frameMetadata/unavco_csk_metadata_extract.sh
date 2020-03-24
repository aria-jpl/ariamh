#!/bin/bash

# export ISCE env
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export FRAMEMETA_HOME=$ARIAMH_HOME/frameMetadata
export PYTHONPATH=/usr/local/isce:$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$PYTHONPATH

source $HOME/verdi/bin/activate

prod_dir=$1
unavco_tgz=$(ls $prod_dir/*.tgz)

# get ASI tarball name
echo "##########################################" 1>&2
echo -n "Getting ASI tarball name: " 1>&2
date 1>&2
asi_name=$(python $FRAMEMETA_HOME/get_asi_tarball_name.py $unavco_tgz)
STATUS=$?
echo -n "Finished getting ASI tarball name: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to get ASI tarball name." 1>&2
  exit 1
fi

# rename UNAVCO tarball to ASI tarball
asi_tgz=$prod_dir/$asi_name
mv -f $unavco_tgz $asi_tgz
if [ $? -ne 0 ]; then
  echo "Failed to rename UNAVCO tarball to ASI tarball." 1>&2
  exit 1
fi

# extract tarball
tar xvfz $asi_tgz -C $prod_dir
if [ $? -ne 0 ]; then
  echo "Failed to extract ASI tarball." 1>&2
  exit 1
fi

# get h5 filename and other vars
h5=$(ls $prod_dir/*.h5)
h5_base=$(basename $h5)
prod_base=$(basename $prod_dir)

# create input xml
echo "##########################################" 1>&2
echo -n "Started  creating input xml file: " 1>&2
date 1>&2
python $FRAMEMETA_HOME/inputFileCreator.py CSK $h5 dummy.raw > inputFileCreator.log 2>&1
STATUS=$?
echo -n "Finished creating input xml file: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to create input xml file." 1>&2
  cat inputFileCreator.log 1>&2
  exit 1
fi

# extract metadata
echo "##########################################" 1>&2
echo -n "Started  extracting metadata: " 1>&2
date 1>&2
python $FRAMEMETA_HOME/datastagerExtractMetadata.py extractorInput.xml > extractMetadata.log 2>&1
STATUS=$?
echo -n "Finished extracting metadata: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to extract metadata." 1>&2
  cat extractMetadata.log 1>&2
  exit 1
fi

# extract quicklook, soft-link and move back to product directory for indexing
png="${h5%.*}.QLK.png"
echo "##########################################" 1>&2
echo -n "Started  extracting quicklook image: " 1>&2
date 1>&2
/usr/bin/python $FRAMEMETA_HOME/csk_h5_quicklook.py $h5 $png > csk_h5_quicklook.log 2>&1
STATUS=$?
echo -n "Finished extracting quicklook image: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to extract quicklook image." 1>&2
  cat csk_h5_quicklook.log 1>&2
  exit 1
fi

# copy browse image
cp $png browse.png
if [ $? -ne 0 ]; then
  echo "Failed to copy quicklook image to browse.png." 1>&2
  exit 1
fi

# create small browse image for viewing quickly over HTTP
convert -resize 250x250 $png browse_small.png
if [ $? -ne 0 ]; then
  echo "Failed to create small quicklook image browse_small.png." 1>&2
  exit 1
fi

# move images to product directory
mv -f browse.png browse_small.png $prod_dir/
if [ $? -ne 0 ]; then
  echo "Failed to add images to product directory." 1>&2
  exit 1
fi

# add dfdn & location metadata
/usr/bin/python $FRAMEMETA_HOME/add_dfdn_metadata.py ${h5}.json ${prod_dir}/DFDN_${h5_base}.xml > add_dfdn_metadata.log 2>&1
if [ $? -ne 0 ]; then
  echo "Failed to add DFDN and location metadata for facets." 1>&2
  cat add_dfdn_metadata.log 1>&2
  exit 1
fi

# add DFAS metadata
/usr/bin/python $FRAMEMETA_HOME/add_dfas_metadata.py ${h5}.json ${prod_dir}/DFAS_*_AccompanyingSheet.xml > add_dfas_metadata.log 2>&1
if [ $? -ne 0 ]; then
  echo "Failed to DFAS metadata for facets." 1>&2
  cat add_dfas_metadata.log 1>&2
  exit 1
fi

# add archive filename to metadata
/usr/bin/python $FRAMEMETA_HOME/set_additional_metadata.py ${prod_dir}/*.tar.gz ${h5}.json > set_additional_metadata.log 2>&1
if [ $? -ne 0 ]; then
  echo "Failed to set additional metadata." 1>&2
  cat set_additional_metadata.log 1>&2
  exit 1
fi

# add dataset type and level to metadata
#python $FRAMEMETA_HOME/set_dataset_metadata.py ${prod_dir} ${h5}.json > set_dataset_metadata.log 2>&1
#if [ $? -ne 0 ]; then
#  echo "Failed to set dataset metadata." 1>&2
#  cat set_dataset_metadata.log 1>&2
#  exit 1
#fi

# add metadata from met.json
if [ -e "_context.json" ]; then
  python $FRAMEMETA_HOME/add_met_json.py _context.json ${h5}.json > add_met_json.log 2>&1
  if [ $? -ne 0 ]; then
    echo "Failed to add met.json to ${h5}.json." 1>&2
    cat add_met_json.log 1>&2
    exit 1
  fi
fi

# move metadata json file
mv -f ${h5}.json ${prod_dir}/${prod_base}.met.json
if [ $? -ne 0 ]; then
  echo "Failed to add product metadata json to product directory." 1>&2
  exit 1
fi

# create product provenance json
/usr/bin/python $FRAMEMETA_HOME/create_prod_prov.py ${prod_dir}/${prod_base}.met.json ${prod_dir}/${prod_base}.prod_prov.json > create_prod_prov.log 2>&1
if [ $? -ne 0 ]; then
  echo "Failed to create prod_prov.json." 1>&2
  cat create_prod_prov.log 1>&2
  exit 1
fi

# create dataset json and clean out metadata json
/usr/bin/python $FRAMEMETA_HOME/create_dataset_json.py ${prod_dir}/${prod_base}.met.json ${prod_dir}/${prod_base}.dataset.json > create_dataset_json.log 2>&1
if [ $? -ne 0 ]; then
  echo "Failed to create dataset.json." 1>&2
  cat create_dataset_json.log 1>&2
  exit 1
fi

# remove h5 file
rm -f $prod_dir/*.h5
