#!/bin/bash
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export DATASTAGER_HOME=$ARIAMH_HOME/datastager

# source environment
source $HOME/verdi/bin/activate

$DATASTAGER_HOME/janitor.py
