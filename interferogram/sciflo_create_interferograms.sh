#!/bin/bash
# export ISCE env
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export TROPMAP_HOME=$HOME/tropmap
export INTERFEROGRAM_HOME=$ARIAMH_HOME/interferogram
export PYTHONPATH=$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$TROPMAP_HOME:$PYTHONPATH
export PATH=$TROPMAP_HOME:$PATH

# source environment
source $HOME/verdi/bin/activate

# get args
id=$1
output_name=$2
project=$3

echo "##########################################" 1>&2
echo -n "Started SciFlo execution of interferogram generation: " 1>&2
date 1>&2
sflExec.py -s -f -o output --args "objectid=$id,output_name=$output_name,project=$project" $INTERFEROGRAM_HOME/CreateInterferogram.sf.xml > sflExec.log 2>&1
STATUS=$?
echo -n "Finished SciFlo execution of interferogram generation: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to create interferogram using SciFlo." 1>&2
  cat sflExec.log 1>&2
  echo "{}"
  exit $STATUS
fi
