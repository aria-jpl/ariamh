#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source veri env
source $HOME/verdi/bin/activate

# source ISCE env
source $HOME/ariamh/isce.sh
export ISCE_HOME=/opt/isce2/isce
export ARIAMH_HOME=$HOME/ariamh
export FRAMEMETA_HOME=$ARIAMH_HOME/frameMetadata
export SENTINEL_HOME=$FRAMEMETA_HOME/sentinel
export PYTHONPATH=/opt/isce2:$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$FRAMEMETA_HOME:$SENTINEL_HOME:$PYTHONPATH

# get union bbox
python $BASE_PATH/get_union_bbox.py $*
