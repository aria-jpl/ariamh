#!/bin/bash

# export ISCE env
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export INTERFEROGRAM_HOME=$ARIAMH_HOME/interferogram
export PYTHONPATH=$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$INTERFEROGRAM_HOME:$PYTHONPATH

PROD=$1
HASH=$2
MET_FILE=${PROD}.met.json
CONTEXT_FILE=`readlink -f context.json`
cd $PROD

# create small browse images for *.browse.png
for i in *.browse.png; do
  small_img=`echo $i | sed 's/\.browse\.png$/.browse_small.png/'`
  convert -resize 250x250 $i $small_img
done

# get browse image
png=`ls *unw.geo.browse.png | head -1`
if [ -z "$png" ]; then
  echo "Failed to find quicklook image." 1>&2
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

# add location metadata
/usr/bin/python $INTERFEROGRAM_HOME/set_additional_metadata.py $MET_FILE $HASH $CONTEXT_FILE
if [ $? -ne 0 ]; then
  echo "Failed to add additional metadata for facets." 1>&2
  echo "{}"
  exit 1
fi
