#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

export GMT_HOME=/usr/local/gmt
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export TROPMAP_HOME=$HOME/tropmap
export UTILS_HOME=$ARIAMH_HOME/utils
export GIANT_HOME=/usr/local/giant/GIAnT
export PYTHONPATH=$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$TROPMAP_HOME:$GIANT_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$TROPMAP_HOME:$GMT_HOME/bin:$PATH


WORK_DIR=`pwd`


echo "##########################################" 1>&2
echo -n "Running pair_selector: " 1>&2
date 1>&2
$BASE_PATH/pair_selector.py $* > pair_selector.log 2>&1
STATUS=$?
echo -n "Finished running pair_selector: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run pair_selector." 1>&2
  cat pair_selector.log 1>&2
  echo "{}"
  exit $STATUS
fi

exit 0
