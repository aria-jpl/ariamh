#!/bin/bash

export ARIAMH_HOME=$HOME/ariamh
export FRAMEMETA_HOME=$ARIAMH_HOME/frameMetadata

source $HOME/verdi/bin/activate

slc_id=$1
request_id=$2

# sling extract to local from scihub
python $FRAMEMETA_HOME/sling_extract_local_scihub.py ${slc_id} ${request_id} > sling_extract_local_scihub.log 2>&1
if [ $? -ne 0 ]; then
  echo "Failed to sling extract to local from scihub." 1>&2
  echo "# ----- errors|exception found in log -----" >> _alt_traceback.txt && grep -i "error\|exception" sling_extract_local_scihub.log >> _alt_traceback.txt
  cat sling_extract_local_scihub.log 1>&2
  echo "{}"
  exit $STATUS
fi
