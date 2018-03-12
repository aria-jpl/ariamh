#!/bin/bash
# export ISCE env
export GMT_HOME=/usr/local/gmt
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export TROPMAP_HOME=$HOME/tropmap
export ML_HOME=$ARIAMH_HOME/ariaml
export UTILS_HOME=$ARIAMH_HOME/utils
export GIANT_HOME=/usr/local/giant/GIAnT
export PYTHONPATH=$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$TROPMAP_HOME:$GIANT_HOME:$PYTHONPATH
export PATH=$TROPMAP_HOME:$GMT_HOME/bin:$PATH


WORK_DIR=`pwd`


# prep inputs
echo "##########################################" 1>&2
echo -n "Preparing inputs: " 1>&2
date 1>&2
/usr/bin/python3 $ML_HOME/trainPredictor_inputPrep.py $ML_HOME _context.json input.json > trainPredictor_inputPrep.log 2>&1
STATUS=$?
echo -n "Finished preparing inputs: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to prepare inputs." 1>&2
  cat trainPredictor_inputPrep.log 1>&2
  echo "{}"
  exit $STATUS
fi


# get product dir
PROD=`ls -d predictor_model-phunw_clfv*`
STATUS=$?
if [ $STATUS -ne 0 ]; then
  echo "Failed to find classifier product." 1>&2
  echo "{}"
  exit $STATUS
fi
cd $PROD


# train predictor
echo "##########################################" 1>&2
echo -n "Training predictor: " 1>&2
date 1>&2
/usr/bin/python3 $ML_HOME/trainPredictor.py input.json > trainPredictor.log 2>&1
STATUS=$?
echo -n "Finished training predictor: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to train predictor." 1>&2
  cat trainPredictor.log 1>&2
  echo "{}"
  exit $STATUS
fi


# cleanup product
rm -rf predictor??????


# move out
cd $WORK_DIR


# create met.json
echo "##########################################" 1>&2
echo -n "Create metadata JSON: " 1>&2
date 1>&2
/usr/bin/python3 $ML_HOME/trainPredictor_met_json.py "$PROD" _context.json "${PROD}/${PROD}.met.json" "${PROD}/${PROD}.dataset.json" > trainPredictor_met_json.log 2>&1
STATUS=$?
echo -n "Finished creating metadata JSON: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to create metadata JSON." 1>&2
  cat trainPredictor_met_json.log 1>&2
  echo "{}"
  exit $STATUS
fi


exit 0
