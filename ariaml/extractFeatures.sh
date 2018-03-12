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
  /usr/bin/python3 $ML_HOME/createInput.py _context.json input.json > createInput.log 2>&1
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
echo -n "Copying stitcher.xml: " 1>&2
date 1>&2
cp -f $ML_HOME/stitcher.xml . > copy_stitcher.log 2>&1
STATUS=$?
echo -n "Finished copying stitcher.xml: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to copy stitcher.xml." 1>&2
  cat copy_stitcher.log 1>&2
  echo "{}"
  exit $STATUS
fi


echo "##########################################" 1>&2
echo -n "Copying swbdStitcher.xml: " 1>&2
date 1>&2
cp -f $ML_HOME/swbdStitcher.xml . > copy_swbdstitcher.log 2>&1
STATUS=$?
echo -n "Finished copying swbdStitcher.xml: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to copy swbdStitcher.xml." 1>&2
  cat copy_swbdstitcher.log 1>&2
  echo "{}"
  exit $STATUS
fi


echo "##########################################" 1>&2
echo -n "Started feature extraction: " 1>&2
date 1>&2
/usr/bin/python3 $ML_HOME/extractFeatures.py input.json > extractFeatures.log 2>&1
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
