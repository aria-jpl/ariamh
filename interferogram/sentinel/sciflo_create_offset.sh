#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source ISCE env
export GMT_HOME=/usr/local/gmt
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export TROPMAP_HOME=$HOME/tropmap
export UTILS_HOME=$ARIAMH_HOME/utils
export GIANT_HOME=/usr/local/giant/GIAnT
export PYTHONPATH=$ISCE_HOME/applications:$ISCE_HOME/components:$BASE_PATH:$ARIAMH_HOME:$TROPMAP_HOME:$GIANT_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$TROPMAP_HOME:$GMT_HOME/bin:$PATH

# source environment
source $HOME/verdi/bin/activate

echo "##########################################" 1>&2
echo -n "Running S1 create offset sciflo: " 1>&2
date 1>&2
/usr/bin/python $BASE_PATH/sciflo_create_offset.py > sciflo_create_offset.log 2>&1
STATUS=$?
echo -n "Finished running S1 create offset sciflo: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run S1 create offset sciflo." 1>&2
  cat sciflo_create_offset.log 1>&2
  echo "{}"
  exit $STATUS
fi
