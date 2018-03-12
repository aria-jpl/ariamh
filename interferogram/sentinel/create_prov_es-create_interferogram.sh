#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# export ISCE env
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh

# source environment
source $HOME/verdi/bin/activate

$BASE_PATH/create_prov_es-create_interferogram.py $*
