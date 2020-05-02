#!/bin/bash
# export ISCE env
export GMT_HOME=/usr/local/gmt
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export TROPMAP_HOME=$HOME/tropmap
export ML_HOME=$ARIAMH_HOME/ariaml
export UTILS_HOME=$ARIAMH_HOME/utils
export GIANT_HOME=/usr/local/giant/GIAnT
export PYTHONPATH=$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$TROPMAP_HOME:$GIANT_HOME:$PYTHONPATH
export PATH=$TROPMAP_HOME:$GMT_HOME/bin:$PATH


# check args
if [ "$#" -eq 1 ]; then

  ifg_url=$1

  echo "##########################################" 1>&2
  echo -n "Create input JSON: " 1>&2
  date 1>&2
  echo "{\"url\": \"$ifg_url\"}" > input.json
  STATUS=$?
  echo -n "Finished creating input JSON: " 1>&2
  date 1>&2
  if [ $STATUS -ne 0 ]; then
    echo "Failed to create input JSON." 1>&2
    echo "{}"
    exit $STATUS
  fi
elif [ "$#" -eq 0 ]; then
  echo "##########################################" 1>&2
  echo -n "Create input JSON: " 1>&2
  date 1>&2
  python $ML_HOME/createInput.py _context.json input.json > createInput.log 2>&1
  STATUS=$?
  echo -n "Finished creating input JSON: " 1>&2
  date 1>&2
  if [ $STATUS -ne 0 ]; then
    echo "Failed to create input JSON." 1>&2
    cat createInput.log 1>&2
    echo "{}"
    exit $STATUS
  fi
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi


echo "##########################################" 1>&2
echo -n "Creating stitcher.xml: " 1>&2
date 1>&2
python $ML_HOME/createStitcherXml.py stitcher.xml > createStitcherXml.log 2>&1
STATUS=$?
echo -n "Finished creating stitcher.xml: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to create stitcher.xml." 1>&2
  cat createStitcherXml.log 1>&2
  echo "{}"
  exit $STATUS
fi


echo "##########################################" 1>&2
echo -n "Creating swbdStitcher.xml: " 1>&2
date 1>&2
python $ML_HOME/createSwbdStitcherXml.py swbdStitcher.xml > createSwbdStitcherXml.log 2>&1
STATUS=$?
echo -n "Finished creating swbdStitcher.xml: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to create swbdStitcher.xml." 1>&2
  cat createSwbdStitcherXml.log 1>&2
  echo "{}"
  exit $STATUS
fi


echo "##########################################" 1>&2
echo -n "Started feature extraction: " 1>&2
date 1>&2
python $ML_HOME/extractFeatures.py input.json > extractFeatures.log 2>&1
STATUS=$?
echo -n "Finished feature extraction: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run feature extraction." 1>&2
  cat extractFeatures.log 1>&2
  echo "{}"
  exit $STATUS
fi


exit 0
