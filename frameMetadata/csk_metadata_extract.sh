#!/bin/bash

# export ISCE env
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export FRAMEMETA_HOME=$ARIAMH_HOME/frameMetadata
export PYTHONPATH=$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$PYTHONPATH

# extract h5 file into working dir
echo "##########################################" 1>&2
echo -n "Started  extracting H5 file: " 1>&2
date 1>&2
tar xfz $1/*.tar.gz
STATUS=$?
echo -n "Finished extracting H5 file: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to untar products." 1>&2
  echo "{}"
  exit 1
fi

# get RAW/SCS h5 file; if none found (raise error)
h5=`ls CSK*.h5`
if [ $? -ne 0 ]; then
  echo "No RAW/SCS data found." 1>&2
  echo "{}"
  exit 1
fi

# create input xml
echo "##########################################" 1>&2
echo -n "Started  creating input xml file: " 1>&2
date 1>&2
/usr/bin/python $FRAMEMETA_HOME/inputFileCreator.py CSK $h5 dummy.raw > inputFileCreator.log 2>&1
STATUS=$?
echo -n "Finished creating input xml file: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to create input xml file." 1>&2
  cat inputFileCreator.log 1>&2
  echo "{}"
  exit 1
fi

# extract metadata
echo "##########################################" 1>&2
echo -n "Started  extracting metadata: " 1>&2
date 1>&2
/usr/bin/python3 $FRAMEMETA_HOME/extractMetadata.py extractorInput.xml > extractMetadata.log 2>&1
STATUS=$?
echo -n "Finished extracting metadata: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to extract metadata." 1>&2
  cat extractMetadata.log 1>&2
  echo "{}"
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
  echo "{}"
  exit 1
fi

# link browse image
ln -sf $png browse.png
if [ $? -ne 0 ]; then
  echo "Failed to link quicklook image to browse.png." 1>&2
  echo "{}"
  exit 1
fi

# create small browse image for viewing quickly over HTTP
convert -resize 250x250 $png browse_small.png
if [ $? -ne 0 ]; then
  echo "Failed to create small quicklook image browse_small.png." 1>&2
  echo "{}"
  exit 1
fi

# move images to product directory
mv -f $png browse.png browse_small.png $1/
if [ $? -ne 0 ]; then
  echo "Failed to add images to product directory." 1>&2
  echo "{}"
  exit 1
fi

# add dfdn & location metadata
/usr/bin/python $FRAMEMETA_HOME/add_dfdn_metadata.py ${h5}.json DFDN_${h5}.xml > add_dfdn_metadata.log 2>&1
if [ $? -ne 0 ]; then
  echo "Failed to add DFDN and location metadata for facets." 1>&2
  cat add_dfdn_metadata.log 1>&2
  echo "{}"
  exit 1
fi


# add DFAS metadata
/usr/bin/python $FRAMEMETA_HOME/add_dfas_metadata.py ${h5}.json DFAS_AccompanyingSheet.xml > add_dfas_metadata.log 2>&1
if [ $? -ne 0 ]; then
  echo "Failed to DFAS metadata for facets." 1>&2
  cat add_dfas_metadata.log 1>&2
  echo "{}"
  exit 1
fi


# add archive filename to metadata
/usr/bin/python $FRAMEMETA_HOME/set_additional_metadata.py $1/*.tar.gz ${h5}.json > set_additional_metadata.log 2>&1
if [ $? -ne 0 ]; then
  echo "Failed to set additional metadata." 1>&2
  cat set_additional_metadata.log 1>&2
  echo "{}"
  exit 1
fi


# print metadata to stdout
cat ${h5}.json
