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

echo "##########################################" 1>&2
echo -n "Running vanilla ISCE sciflo: " 1>&2
date 1>&2
/usr/bin/python $INTERFEROGRAM_HOME/sciflo_vanilla_isce.py > sciflo_vanilla_isce.log 2>&1
STATUS=$?
echo -n "Finished running vanilla ISCE sciflo: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run vanilla ISCE sciflo." 1>&2
  cat sciflo_vanilla_isce.log 1>&2
  echo "{}"
  exit $STATUS
fi
