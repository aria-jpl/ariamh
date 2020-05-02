#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source ISCE env
export GMT_HOME=/usr/local/gmt
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export TROPMAP_HOME=$HOME/tropmap
export UTILS_HOME=$ARIAMH_HOME/utils
export GIANT_HOME=/usr/local/giant/GIAnT
#export WORK_DIR=$PWD
#echo $WORK_DIR

#export LD_LIBRARY_PATH=/usr/local/gdal/lib:$LD_LIBRARY_PATH
#export GDAL_DATA=/usr/local/gdal/share/gdal

export PYTHONPATH=.:$ISCE_HOME/applications:$ISCE_HOME/components:$BASE_PATH:$ARIAMH_HOME:$ARIAMH_HOME/interferogram:$TROPMAP_HOME:$GIANT_HOME:$UTILS_HOME:$PYTHONPATH
export PATH=$ISCE_HOME/applications:$ISCE_HOME/bin:/usr/local/gdal/bin:$ISCE_HOME/bin:/opt/conda/bin/:/opt/conda/pkgs/libgdal-2.3.2-h9d4a965_0/bin:$PATH
export PATH=$BASE_PATH:$TROPMAP_HOME:$GMT_HOME/bin:$PATH
echo $PYTHONPATH
# source environment
source $HOME/verdi/bin/activate

python3 /home/ops/ariamh/interferogram/alos/makeGeocube.py -m reference -s secondary -o metadata.h5
