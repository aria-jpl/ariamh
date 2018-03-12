#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source veri env
source $HOME/verdi/bin/activate

# source ISCE env
source /etc/profile.d/isce.sh
export ISCE_HOME=/usr/local/isce/isce
export ARIAMH_HOME=$HOME/ariamh
export FRAMEMETA_HOME=$ARIAMH_HOME/frameMetadata
export PYTHONPATH=/usr/local/isce:$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$FRAMEMETA_HOME:$FRAMEMETA_HOME/sentinel:$PYTHONPATH

# get union bbox
/usr/bin/python3 $BASE_PATH/get_union_bbox.py $*
