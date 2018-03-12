#!/bin/bash

# export ISCE env
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export NETWORK_SELECTOR_HOME=$ARIAMH_HOME/network_selector
export FRAMEMETA_HOME=$ARIAMH_HOME/frameMetadata
export UTILS_HOME=$ARIAMH_HOME/utils
export PYTHONPATH=$UTILS_HOME:$FRAMEMETA_HOME:$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$PYTHONPATH

jd_json_file="job_description.json"

echo "##########################################" 1>&2
echo -n "Write inputs to job description JSON: " 1>&2
date 1>&2
/usr/bin/python3 $NETWORK_SELECTOR_HOME/write_job_description.py --file $jd_json_file --context context.json > jobDescriptorWriter.log 2>&1
STATUS=$?
echo -n "Finished writing inputs to job description JSON: " 1>&2
date 1>&2
if [ $STATUS -lt 0 ]; then
  echo "Failed to write inputs to job description JSON." 1>&2
  cat jobDescriptorWriter.log 1>&2
  echo "{}"
  exit $STATUS
fi
