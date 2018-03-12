#!/bin/bash

# sample usage:
# ./run_network_selector_by_id.sh CSKS1_RAW_B_HI_05_HH_RA_20130620133123_20130620133129 test.out

# export ISCE env
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export NETWORK_SELECTOR_HOME=$ARIAMH_HOME/network_selector
export FRAMEMETA_HOME=$ARIAMH_HOME/frameMetadata
export UTILS_HOME=$ARIAMH_HOME/utils
export PYTHONPATH=$FRAMEMETA_HOME:$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$PYTHONPATH

id=$1
output_file=$2
project=$3
jd_json_file=$4

echo "##########################################" 1>&2
echo -n "Getting metadata json by ID: " 1>&2
date 1>&2
/usr/bin/python $NETWORK_SELECTOR_HOME/getMetadata.py $id metadata.json > getMetadata.log 2>&1
STATUS=$?
echo -n "Finished getting metadata json by ID: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to get metadata json by ID" 1>&2
  cat getMetadata.log 1>&2
  echo "{}"
  exit $STATUS
fi

echo "##########################################" 1>&2
echo -n "Write inputs to job description JSON: " 1>&2
date 1>&2
/usr/bin/python3 $UTILS_HOME/jobDescriptorWriter.py --file $jd_json_file \
  --update networkSelector "{\"inputFile\":\"metadata.json\"}" > jobDescriptorWriter.log 2>&1
STATUS=$?
echo -n "Finished writing inputs to job description JSON: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to write inputs to job description JSON." 1>&2
  cat jobDescriptorWriter.log 1>&2
  echo "{}"
  exit $STATUS
fi

echo "##########################################" 1>&2
echo -n "Started network selector: " 1>&2
date 1>&2
/usr/bin/python3 $NETWORK_SELECTOR_HOME/networkSelector.py $jd_json_file > networkSelector.log 2>&1
STATUS=$?
echo -n "Finished network selector: " 1>&2
date 1>&2
if [ $STATUS -eq 255 ]; then
  echo "Failed to run network selector." 1>&2
  cat networkSelector.log 1>&2
  echo "{}"
  exit $STATUS
fi

echo "##########################################" 1>&2
echo -n "Create job description for each network selector file: " 1>&2
date 1>&2
for i in ${output_file}_*; do
  if [ -e "$i" ]; then
    new_jd_json_file=${jd_json_file}_${i##*_}
    cp -f $jd_json_file $new_jd_json_file
    /usr/bin/python3 $UTILS_HOME/jobDescriptorWriter.py --file $new_jd_json_file \
      --update networkSelector "{\"outputFile\":\"$i\"}" \
      --update createInterferogram "{\"inputFile\":\"$i\"}" >> jobDescriptorWriter.log 2>&1
  fi
done

echo "##########################################" 1>&2
echo -n "Writing context.json: " 1>&2
date 1>&2
/usr/bin/python $NETWORK_SELECTOR_HOME/writeContextJson.py $id $output_file $jd_json_file $STATUS context.json results.txt > writeContextJson.log 2>&1
STATUS=$?
echo -n "Finished writing context.json: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to write context.json." 1>&2
  cat writeContextJson.log 1>&2
  echo "{}"
  exit $STATUS
fi

# print out results
cat results.txt
