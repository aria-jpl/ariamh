#!/bin/bash
# export ISCE env
export GMT_HOME=/usr/local/gmt
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export TROPMAP_HOME=$HOME/tropmap
export INTERFEROGRAM_HOME=$ARIAMH_HOME/interferogram
export GIANT_HOME=/usr/local/giant/GIAnT
export PYTHONPATH=$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$TROPMAP_HOME:$GIANT_HOME:$PYTHONPATH
export PATH=$TROPMAP_HOME:$GMT_HOME/bin:$PATH


echo "##########################################" 1>&2
echo -n "Running preflight: " 1>&2
date 1>&2
/usr/bin/python $INTERFEROGRAM_HOME/run_preflight.py --results results.json $* > run_preflight.log 2>&1
STATUS=$?
echo -n "Finished running preflight: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run preflight." 1>&2
  cat run_preflight.log 1>&2
  echo "{}"
  exit $STATUS
fi

cat results.json
