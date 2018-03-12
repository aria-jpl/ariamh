#!/usr/bin/env bash

# export ISCE env
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export FRAMEMETA_HOME=$ARIAMH_HOME/frameMetadata
export PYTHONPATH=/usr/local/isce:$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$PYTHONPATH

source $HOME/verdi/bin/activate

ID=$1
URL=$2
JOB_DIR=$PWD
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# localize datastager product - No longer needed since localization done by worker
#echo "##########################################" 1>&2
#echo -n "Localizing $URL: " 1>&2
#date 1>&2
#$BASE_PATH/localize_product.py $URL
#STATUS=$?
#echo -n "Finished localizing $URL: " 1>&2
#date 1>&2
#if [ $STATUS -ne 0 ]; then
#  echo "Failed to localize $URL." 1>&2
#  exit 1
#fi

PROD_DIR="${JOB_DIR}/${ID}"

# cd to product dir
cd $PROD_DIR

# source job rc file
source job.rc

# set variables
read YEAR MONTH DAY <<< $(echo ${ID} | sed -r "s/EL([0-9]{4})([0-9]{2})([0-9]{2})_.+/\1 \2 \3/")
FILE="$INCOMING_PATH"
DATA_WORKING_DIR="${WORKING_DIR}/${YEAR}/${MONTH}/${DAY}/${ID}"
DATA_OUTGOING_DIR="${OUTGOING_DIR}/${YEAR}/${MONTH}/${DAY}/${ID}"
DATA_DONE_DIR="${DONE_DIR}/${YEAR}/${MONTH}/${DAY}/${ID}"

# extract tarball
echo "##########################################" 1>&2
echo -n "Started extracting tarball: " 1>&2
date 1>&2
OUTPUT="$(tar xfz *.tar.gz 2>&1)"
STATUS=$?
echo -n "Finished extracting tarball: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "${OUTPUT}" 1>&2
  echo "Failed to untar products." 1>&2
  FLAG="FLAG_BAD_TARGZ"
  DATA_FAILED_DIR="${FAILED_DIR}/$(date +%Y%m%dT%H%M%S)-${ID}--${FLAG}"
  fab -f $BASE_PATH/fab_funcs.py move_remote_path:$INCOMING_HOST,$DATA_WORKING_DIR,$DATA_FAILED_DIR
  if [ "$?" -ne 0 ]; then
    echo "Failed to move $DATA_WORKING_DIR to $DATA_FAILED_DIR on $INCOMING_HOST." 1>&2
    exit 1
  fi
  echo "$(date) - Moved contents to ${DATA_FAILED_DIR}" 1>&2
   
  # flag source data file
  FILE_FLAGGED="${FILE}--${FLAG}"
  fab -f $BASE_PATH/fab_funcs.py move_remote_path:$INCOMING_HOST,$FILE,$FILE_FLAGGED
  if [ "$?" -ne 0 ]; then
    echo "Failed to move $FILE to $FILE_FLAGGED on $INCOMING_HOST." 1>&2
    exit 1
  fi
  echo "$(date) - Renamed file to ${FILE_FLAGGED}" 1>&2

  # delete product in datastager dir
  fab -f $BASE_PATH/fab_funcs.py remove_remote_path:$INCOMING_HOST,$DATA_OUTGOING_DIR
  if [ "$?" -ne 0 ]; then
    echo "Failed to remove $DATA_OUTGOING_DIR on $INCOMING_HOST." 1>&2
    exit 1
  fi
  echo "$(date) - Removed ${DATA_OUTGOING_DIR}" 1>&2

  # email notification
  if [ -n "${NOTIFICATION_EMAIL_TO}" ]; then
    BODY="${DATA_FAILED_DIR}
-------------
${OUTPUT}
-------------
$(fab -f $BASE_PATH/fab_funcs.py ls_la:$INCOMING_HOST,$DATA_FAILED_DIR)
-------------"
    echo "${BODY}" | mail  -s "(handler_file) Unable to decompress \"${FILE}\""  -r "${NOTIFICATION_EMAIL_FROM}"  "${NOTIFICATION_EMAIL_TO}"
    echo "$(date) - sent notification to \"${NOTIFICATION_EMAIL_TO}\""
  fi

  exit 1
fi


# ------------------------------------------------------------------------------
# verify existence of HDF5

# determine the single h5 file
H5_FILEPATH="$(ls ${PROD_DIR}/*h5)"
if [ -n "${H5_FILEPATH}" ]; then
    echo "$(date) - Found h5 file \"${H5_FILEPATH}\"."

    # the basename of the h5 file
    H5_FILENAME="$(basename ${H5_FILEPATH})"

else
    echo "$(date) - Unable to find h5 file inside ${PROD_DIR}." 1>&2
    ls -la ${PROD_DIR}

    FLAG="FLAG_MISSING_H5"

    DATA_FAILED_DIR="${FAILED_DIR}/$(date +%Y%m%dT%H%M%S)-${BASENAME_NOEXT}--${FLAG}"
    fab -f $BASE_PATH/fab_funcs.py move_remote_path:$INCOMING_HOST,$DATA_WORKING_DIR,$DATA_FAILED_DIR
    if [ "$?" -ne 0 ]; then
      echo "Failed to move $DATA_WORKING_DIR to $DATA_FAILED_DIR on $INCOMING_HOST." 1>&2
      exit 1
    fi
    echo "$(date) - Moved contents to ${DATA_FAILED_DIR}" 1>&2

    # flag source data file
    FILE_FLAGGED="${FILE}--${FLAG}"
    fab -f $BASE_PATH/fab_funcs.py move_remote_path:$INCOMING_HOST,$FILE,$FILE_FLAGGED
    if [ "$?" -ne 0 ]; then
      echo "Failed to move $FILE to $FILE_FLAGGED on $INCOMING_HOST." 1>&2
      exit 1
    fi
    echo "$(date) - Renamed file to ${FILE_FLAGGED}" 1>&2

    # delete product in datastager dir
    fab -f $BASE_PATH/fab_funcs.py remove_remote_path:$INCOMING_HOST,$DATA_OUTGOING_DIR
    if [ "$?" -ne 0 ]; then
      echo "Failed to remove $DATA_OUTGOING_DIR on $INCOMING_HOST." 1>&2
      exit 1
    fi
    echo "$(date) - Removed ${DATA_OUTGOING_DIR}" 1>&2

    # email notification
    if [ -n "${NOTIFICATION_EMAIL_TO}" ]; then
        BODY="contents of ${DATA_FAILED_DIR} :
-------------
$(fab -f $BASE_PATH/fab_funcs.py ls_la:$INCOMING_HOST,$DATA_FAILED_DIR)
-------------"
        echo "${BODY}" | mail  -s "(handler_file) Unable to find h5 file inside \"${FILE}\""  -r "${NOTIFICATION_EMAIL_FROM}"  "${NOTIFICATION_EMAIL_TO}"
        echo "$(date) - sent notification to \"${NOTIFICATION_EMAIL_TO}\""
    fi

    exit 1
fi


# ------------------------------------------------------------------------------
# for now, only interested in CSK RAW or SCS files

# want:   CSKS4_RAW_B_HI_03_HH_RA_SF_20130713133755_20130713133802.h5 or
#         CSKS3_SCS_B_S2_20_HH_RD_SF_20120514010036_20120514010044.h5

# check if have RAW or SCS h5 file
RAW_H5_FILEPATH="$(ls ${PROD_DIR}/*_RAW_*h5)"
SCS_H5_FILEPATH="$(ls ${PROD_DIR}/*_SCS_*h5)"

if [ -n "${SCS_H5_FILEPATH}" ]; then
    echo "$(date) - is SCS h5 file \"${H5_FILEPATH}\"."
else
    echo "$(date) - No SCS file found in ${PROD_DIR}." 1>&2

    if [ -n "${RAW_H5_FILEPATH}" ]; then
        echo "$(date) - is RAW h5 file \"${H5_FILEPATH}\"."
    else
        echo "$(date) - No RAW file found in ${PROD_DIR}." 1>&2
        ls -la ${PROD_DIR}
    
        # clean up files
        fab -f $BASE_PATH/fab_funcs.py remove_remote_path_glob:$INCOMING_HOST,$DATA_WORKING_DIR
        if [ "$?" -ne 0 ]; then
          echo "Failed to rm -rf ${DATA_WORKING_DIR}/* on $INCOMING_HOST." 1>&2
          exit 1
        fi
    
        echo "$(date) - Done with ${DATA_WORKING_DIR}. moving to ${DATA_DONE_DIR} ..."
        fab -f $BASE_PATH/fab_funcs.py move_remote_path:$INCOMING_HOST,$DATA_WORKING_DIR,$DATA_DONE_DIR
        if [ "$?" -ne 0 ]; then
          echo "Failed to move $DATA_WORKING_DIR to $DATA_DONE_DIR on $INCOMING_HOST." 1>&2
          exit 1
        fi
    
        # delete product in datastager dir
        fab -f $BASE_PATH/fab_funcs.py remove_remote_path:$INCOMING_HOST,$DATA_OUTGOING_DIR
        if [ "$?" -ne 0 ]; then
          echo "Failed to remove $DATA_OUTGOING_DIR on $INCOMING_HOST." 1>&2
          exit 1
        fi
        echo "$(date) - Removed ${DATA_OUTGOING_DIR}" 1>&2
    
        exit 0
    fi
fi


# ------------------------------------------------------------------------------
# extract metadata from the HDF5

H5_XML_FILEPATH="${PROD_DIR}/${H5_FILENAME}.xml"

# dump xml metadata from h5
OUTPUT="$(h5dump --xml --header ${H5_FILEPATH} > ${H5_XML_FILEPATH}  2>&1)"
if [ "$?" -eq "0" ]; then
    echo "$(date) - Extracted metadata from ${H5_FILEPATH} to ${H5_XML_FILEPATH}."

else
    echo "${OUTPUT}" 1>&2
    echo "$(date) - Unable to extract metadata from ${H5_FILEPATH}." 1>&2

    FLAG="FLAG_BAD_H5_XML"

    DATA_FAILED_DIR="${FAILED_DIR}/$(date +%Y%m%dT%H%M%S)-${BASENAME_NOEXT}--${FLAG}"
    fab -f $BASE_PATH/fab_funcs.py move_remote_path:$INCOMING_HOST,$DATA_WORKING_DIR,$DATA_FAILED_DIR
    if [ "$?" -ne 0 ]; then
      echo "Failed to move $DATA_WORKING_DIR to $DATA_FAILED_DIR on $INCOMING_HOST." 1>&2
      exit 1
    fi
    echo "$(date) - Moved contents to ${DATA_FAILED_DIR}" 1>&2

    # flag source data file
    FILE_FLAGGED="${FILE}--${FLAG}"
    fab -f $BASE_PATH/fab_funcs.py move_remote_path:$INCOMING_HOST,$FILE,$FILE_FLAGGED
    if [ "$?" -ne 0 ]; then
      echo "Failed to move $FILE to $FILE_FLAGGED on $INCOMING_HOST." 1>&2
      exit 1
    fi
    echo "$(date) - Renamed file to ${FILE_FLAGGED}" 1>&2

    # delete product in datastager dir
    fab -f $BASE_PATH/fab_funcs.py remove_remote_path:$INCOMING_HOST,$DATA_OUTGOING_DIR
    if [ "$?" -ne 0 ]; then
      echo "Failed to remove $DATA_OUTGOING_DIR on $INCOMING_HOST." 1>&2
      exit 1
    fi
    echo "$(date) - Removed ${DATA_OUTGOING_DIR}" 1>&2

    # email notification
    if [ -n "${NOTIFICATION_EMAIL_TO}" ]; then
        BODY="Unable to extract metadata using: h5dump --xml --header \"${H5_FILEPATH}\"
-------------
${OUTPUT}
-------------
$(ls -la ${PROD_DIR})
-------------"
        echo "${BODY}" | mail  -s "(handler_file) Unable to extract metadata from \"${H5_FILEPATH}\""  -r "${NOTIFICATION_EMAIL_FROM}"  "${NOTIFICATION_EMAIL_TO}"
        echo "$(date) - sent notification to \"${NOTIFICATION_EMAIL_TO}\""
    fi

    exit 1
fi


# run metadata extraction
cd $JOB_DIR
FRAMEMETA_PATH=$(dirname "${BASE_PATH}")/frameMetadata
OUTPUT="$(${FRAMEMETA_PATH}/datastager_csk_metadata_extract.sh $H5_FILEPATH 2>&1)"
if [ "$?" -eq "0" ]; then
    echo "$(date) - Extracted grq metadata from ${H5_FILEPATH} to ${H5_XML_FILEPATH}."

else
    echo "${OUTPUT}" 1>&2
    echo "$(date) - Unable to extract grq metadata from ${H5_FILEPATH}." 1>&2

    FLAG="FLAG_GRQ_METADATA_EXTRACT_FAILED"

    DATA_FAILED_DIR="${FAILED_DIR}/$(date +%Y%m%dT%H%M%S)-${BASENAME_NOEXT}--${FLAG}"
    fab -f $BASE_PATH/fab_funcs.py move_remote_path:$INCOMING_HOST,$DATA_WORKING_DIR,$DATA_FAILED_DIR
    if [ "$?" -ne 0 ]; then
      echo "Failed to move $DATA_WORKING_DIR to $DATA_FAILED_DIR on $INCOMING_HOST." 1>&2
      exit 1
    fi
    echo "$(date) - Moved contents to ${DATA_FAILED_DIR}" 1>&2

    # flag source data file
    FILE_FLAGGED="${FILE}--${FLAG}"
    fab -f $BASE_PATH/fab_funcs.py move_remote_path:$INCOMING_HOST,$FILE,$FILE_FLAGGED
    if [ "$?" -ne 0 ]; then
      echo "Failed to move $FILE to $FILE_FLAGGED on $INCOMING_HOST." 1>&2
      exit 1
    fi
    echo "$(date) - Renamed file to ${FILE_FLAGGED}" 1>&2

    # delete product in datastager dir
    fab -f $BASE_PATH/fab_funcs.py remove_remote_path:$INCOMING_HOST,$DATA_OUTGOING_DIR
    if [ "$?" -ne 0 ]; then
      echo "Failed to remove $DATA_OUTGOING_DIR on $INCOMING_HOST." 1>&2
      exit 1
    fi
    echo "$(date) - Removed ${DATA_OUTGOING_DIR}" 1>&2

    # email notification
    if [ -n "${NOTIFICATION_EMAIL_TO}" ]; then
        BODY="Unable to extract grq metadata for \"${H5_FILEPATH}\"
-------------
${OUTPUT}
-------------
$(ls -la ${PROD_DIR})
-------------"
        echo "${BODY}" | mail  -s "(handler_file) Unable to extract grq metadata for \"${H5_FILEPATH}\""  -r "${NOTIFICATION_EMAIL_FROM}"  "${NOTIFICATION_EMAIL_TO}"
        echo "$(date) - sent notification to \"${NOTIFICATION_EMAIL_TO}\""
    fi

    exit 1
fi


# remove h5 files since tarball is included already
rm -f $PROD_DIR/*h5

# remove job.rc file
rm -f $PROD_DIR/job.rc

# remove met.json file
rm -f $PROD_DIR/met.json


# ------------------------------------------------------------------------------
# done with data. move to done.

# clean up files
fab -f $BASE_PATH/fab_funcs.py remove_remote_path_glob:$INCOMING_HOST,$DATA_WORKING_DIR
if [ "$?" -ne 0 ]; then
  echo "Failed to rm -rf ${DATA_WORKING_DIR}/* on $INCOMING_HOST." 1>&2
  exit 1
fi

echo "$(date) - Done with ${DATA_WORKING_DIR}. moving to ${DATA_DONE_DIR} ..."
fab -f $BASE_PATH/fab_funcs.py move_remote_path:$INCOMING_HOST,$DATA_WORKING_DIR,$DATA_DONE_DIR
if [ "$?" -ne 0 ]; then
  echo "Failed to move $DATA_WORKING_DIR to $DATA_DONE_DIR on $INCOMING_HOST." 1>&2
  exit 1
fi

# delete product in datastager dir
fab -f $BASE_PATH/fab_funcs.py remove_remote_path:$INCOMING_HOST,$DATA_OUTGOING_DIR
if [ "$?" -ne 0 ]; then
  echo "Failed to remove $DATA_OUTGOING_DIR on $INCOMING_HOST." 1>&2
  exit 1
fi
echo "$(date) - Removed ${DATA_OUTGOING_DIR}" 1>&2

# ------------------------------------------------------------------------------
# delete previously failed files

fab -f $BASE_PATH/fab_funcs.py remove_remote_path:$INCOMING_HOST,${FILE}--FLAG*
if [ "$?" -ne 0 ]; then
  echo "Failed to remove ${FILE}--FLAG* on $INCOMING_HOST." 1>&2
  exit 1
fi

# write PROV-ES JSON
$BASE_PATH/create_prov_es.py "$ID" "$URL" "$PROD_DIR" "$PROD_DIR/${ID}.prov_es.json"
if [ "$?" -ne 0 ]; then
  echo "Failed to write PROV-ES JSON." 1>&2
  exit 1
fi
