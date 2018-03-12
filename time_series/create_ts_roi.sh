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
echo -n "Running time-series generation: " 1>&2
date 1>&2
python3 $BASE_PATH/create_ts_roi.py _context.json > create_ts_roi.log 2>&1
STATUS=$?
echo -n "Finished running time-series generation: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run time-series generation." 1>&2
  cat create_ts_roi.log 1>&2
  echo "{}"
  exit $STATUS
fi
