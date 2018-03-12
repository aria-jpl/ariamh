#!/bin/bash

# export ISCE env
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export FRAMEMETA_HOME=$ARIAMH_HOME/frameMetadata
export PYTHONPATH=/usr/local/isce:$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$PYTHONPATH

# create input xml
img=`ls $1/IMG-HH-*0__?`
led=`echo $img | sed 's/IMG-HH/LED/'`
echo "##########################################" 1>&2
echo -n "Started  creating input xml file: " 1>&2
date 1>&2
/usr/bin/python3 $FRAMEMETA_HOME/inputFileCreator.py Alos $img $led dummy.raw > inputFileCreator.log 2>&1
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

# print metadata to stdout
cat ${img}.json
