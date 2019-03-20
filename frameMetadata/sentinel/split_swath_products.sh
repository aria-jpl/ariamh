#!/usr/bin/env bash

source $HOME/verdi/bin/activate

PROD_DIR=$1
JOB_DIR=$PWD
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)
echo "PROD_DIR : " $PROD_DIR 1>&2
# product ID
PROD_ID=`basename ${PROD_DIR}`
echo "PROD_ID : " $PROD_ID 1>&2

# cd to product dir
cd ${PROD_DIR}
echo "WORKING DIR : " $PWD 1>&2

# zip file
ZIP_FILE=`ls *.zip`
echo "ZIP_FILE : " ${ZIP_FILE} 1>&2

STATUS=$?
echo "STATUS BEFORE SPLITTING SWATH : " $STATUS1>&2
if [ $STATUS -ne 0 ]; then
  STATUS = 0
fi

# split swath products
echo "##########################################" 1>&2
echo -n "Started splitting swath products from SLC: " 1>&2
date 1>&2
${BASE_PATH}/split_swath_products.py ${ZIP_FILE} $JOB_DIR > \
                                     ${JOB_DIR}/split_swath_products.log 2>&1
STATUS=$?
echo -n "Finished splitting swath products from SLC: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to split swath products from SLC." 1>&2
  #exit 1
fi

# extract metadata for each swath
cd ..
#############################################
# disable extraction of subswath dataset
#############################################
#for swath_dir in swaths/*; do
#  id=`basename $swath_dir`
#
#  echo "##########################################" 1>&2
#  echo -n "Started extracting swath metadata: " 1>&2
#  date 1>&2
#  ${BASE_PATH}/extractMetadata_s1.sh -i ${swath_dir}/${id}.xml \
#                                     -o ${swath_dir}/met.json > \
#                                     ${JOB_DIR}/extractMetadata_s1_${id}.log 2>&1
#  STATUS=$?
#  echo -n "Finished extracting swath metadata: " 1>&2
#  date 1>&2
#  if [ $STATUS -ne 0 ]; then
#    echo "Failed to extract swath metadata." 1>&2
#    exit 1
#  fi
#
#  echo "##########################################" 1>&2
#  echo -n "Creating swath metadata: " 1>&2
#  date 1>&2
#  ${BASE_PATH}/create_met_json_swath.py _context.json \
#                                        ${PROD_DIR}/${PROD_ID}.met.json \
#                                        ${swath_dir}/met.json \
#                                        ${swath_dir}/${id}.met.json > \
#                                        ${JOB_DIR}/create_met_json_swath_${id}.log 2>&1
#  STATUS=$?
#  echo -n "Finished creating swath metadata: " 1>&2
#  date 1>&2
#  if [ $STATUS -ne 0 ]; then
#    echo "Failed to create swath metadata." 1>&2
#    exit 1
#  fi
#
#  # write PROV-ES for subswath product
#  echo "##########################################" 1>&2
#  echo -n "Creating swath provenance: " 1>&2
#  date 1>&2
#  $BASE_PATH/create_prov_es_swath.py _context.json "$id" \
#                                     "$swath_dir" \
#                                     "$swath_dir/${id}.prov_es.json" > \
#                                     ${JOB_DIR}/create_prov_json_swath_${id}.log 2>&1
#  STATUS=$?
#  echo -n "Finished creating swath provenance: " 1>&2
#  date 1>&2
#  if [ $STATUS -ne 0 ]; then
#    echo "Failed to create swath provenance." 1>&2
#    exit 1
#  fi
#
#  # write swath dataset JSON
#  echo "##########################################" 1>&2
#  echo -n "Creating swath dataset JSON: " 1>&2
#  date 1>&2
#  ${BASE_PATH}/create_dataset_swath.py "$id" \
#                                       ${swath_dir}/met.json \
#                                       ${swath_dir}/${id}.dataset.json > \
#                                       ${JOB_DIR}/create_dataset_swath_${id}.log 2>&1
#  STATUS=$?
#  echo -n "Finished creating swath dataset JSON: " 1>&2
#  date 1>&2
#  if [ $STATUS -ne 0 ]; then
#    echo "Failed to create swath dataset JSON." 1>&2
#    exit 1
#  fi
#
#  rm -rf ${swath_dir}/met.json
#done

# write PROV-ES for zip file product
echo "##########################################" 1>&2
echo -n "Creating SLC provenance: " 1>&2
date 1>&2
${BASE_PATH}/create_prov_es.py _context.json "$PROD_DIR" \
                               "${PROD_DIR}/${PROD_ID}.prov_es.json" > \
                               ${JOB_DIR}/create_prov_es.log 2>&1
STATUS=$?
echo -n "Finished creating SLC provenance: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to create SLC provenance." 1>&2
  exit 1
fi

# remove zip file
#rm -rf ${PROD_DIR}/${ZIP_FILE}

exit $?
