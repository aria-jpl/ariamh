#!/bin/bash
# export ISCE env
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export INTERFEROGRAM_HOME=$ARIAMH_HOME/interferogram

# source environment
source $HOME/verdi/bin/activate

$INTERFEROGRAM_HOME/create_prov_es-create_interferogram.py $*
