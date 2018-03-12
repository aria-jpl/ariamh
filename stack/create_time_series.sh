#!/bin/bash
# export ISCE env
export GMT_HOME=/usr/local/gmt
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export TROPMAP_HOME=$HOME/tropmap
export STACK_HOME=$ARIAMH_HOME/stack
export UTILS_HOME=$ARIAMH_HOME/utils
export GIANT_HOME=/usr/local/giant/GIAnT
export PYTHONPATH=$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$TROPMAP_HOME:$GIANT_HOME:$PYTHONPATH
export PATH=$ISCE_HOME/bin:$ISCE_HOME/applications:$STACK_HOME:$STACK_HOME/stack:$TROPMAP_HOME:$GMT_HOME/bin:$PATH

WORK_DIR=`pwd`

echo "##########################################" 1>&2
echo -n "Generate runStack input JSON file: " 1>&2
date 1>&2
/usr/bin/python $STACK_HOME/createRunStackInput.py context.json runStack_metadata.json runStack_input.json > createRunStackInput.log 2>&1
STATUS=$?
echo -n "Finished generating runStack input JSON file: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to generate runStack input JSON file." 1>&2
  cat createRunStackInput.log 1>&2
  echo "{}"
  exit $STATUS
fi

echo "##########################################" 1>&2
echo -n "Running runStack.py: " 1>&2
date 1>&2
xvfb-run /usr/bin/python $STACK_HOME/runStack.py runStack_input.json > runStack.log 2>&1
STATUS=$?
echo -n "Finished runStack.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run runStack.py." 1>&2
  cat runStack.log 1>&2
  echo "{}"
  exit $STATUS
fi

# get product ID
PRODUCT_ID="$(ls -d time-series_*)"

echo "##########################################" 1>&2
echo -n "Updating time-series product metadata: " 1>&2
date 1>&2
/usr/bin/python $STACK_HOME/update_time_series_metadata.py ${PRODUCT_ID}/runStack_metadata.json ${PRODUCT_ID}/*.met.json ${PRODUCT_ID} > update_time_series_metadata.log 2>&1
STATUS=$?
echo -n "Finished updating time-series product metadata: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to update time-series product metadata." 1>&2
  cat update_time_series_metadata.log 1>&2
  echo "{}"
  exit $STATUS
fi

exit 0
