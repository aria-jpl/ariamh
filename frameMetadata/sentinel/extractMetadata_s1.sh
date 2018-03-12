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
export SENTINEL_HOME=$FRAMEMETA_HOME/sentinel
export PYTHONPATH=/usr/local/isce:$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$FRAMEMETA_HOME:$PYTHONPATH

# create input xml
/usr/bin/python3 $BASE_PATH/extractMetadata_s1.py $*
