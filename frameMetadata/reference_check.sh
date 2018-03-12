#!/bin/bash

# export ISCE env
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export FRAMEMETA_HOME=$ARIAMH_HOME/frameMetadata
export PYTHONPATH=$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$PYTHONPATH

dataset=$1
metadata_file=$2
reference_check_json=$3

# run reference checker
echo "##########################################" 1>&2
echo -n "Started reference checker: " 1>&2
date 1>&2
/usr/bin/python $FRAMEMETA_HOME/reference_check.py $dataset $metadata_file $reference_check_json > reference_check.log 2>&1
STATUS=$?
echo -n "Finished reference checker: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to validate reference check." 1>&2
  cat reference_check.log 1>&2
  exit 1
fi
