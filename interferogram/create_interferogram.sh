#!/bin/bash
# export ISCE env
export GMT_HOME=/usr/local/gmt
export ARIAMH_HOME=$HOME/ariamh
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export TROPMAP_HOME=$HOME/tropmap
export INTERFEROGRAM_HOME=$ARIAMH_HOME/interferogram
export UTILS_HOME=$ARIAMH_HOME/utils
export GIANT_HOME=/usr/local/giant/GIAnT
export PYTHONPATH=$ISCE_HOME/applications:$ISCE_HOME/components:$ARIAMH_HOME:$TROPMAP_HOME:$GIANT_HOME:$PYTHONPATH
export PATH=$TROPMAP_HOME:$GMT_HOME/bin:$PATH

netsel_file=$1
jobdesc_file=$2
project=$3

WORK_DIR=`pwd`

echo "##########################################" 1>&2
echo -n "Generate network selector input hash ID: " 1>&2
date 1>&2
/usr/bin/python $INTERFEROGRAM_HOME/getInputHash.py $netsel_file > getInputHash.log 2>&1
STATUS=$?
echo -n "Finished generating network selector input hash ID: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to generate network selector input hash ID." 1>&2
  cat getInputHash.log 1>&2
  echo "{}"
  exit $STATUS
fi

# get hash id
HASH=`cat netsel_hash.txt`
STATUS=$?
if [ $STATUS -ne 0 ]; then
  echo "Failed to find network selector hash text file." 1>&2
  echo "{}"
  exit $STATUS
fi

echo "##########################################" 1>&2
echo -n "Running interferogram generation check: " 1>&2
date 1>&2
/usr/bin/python $INTERFEROGRAM_HOME/checkInterferogramByInputHash.py $HASH > checkInterferogramByInputHash.log 2>&1
STATUS=$?
echo -n "Finished interferogram generation check: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run interferogram generation check." 1>&2
  cat checkInterferogramByInputHash.log 1>&2
  echo "{}"
  exit $STATUS
fi

# skip this job if already processed
TOTAL=`head -1 interferograms_found.txt`
STATUS=$?
if [ $STATUS -ne 0 ]; then
  echo "Failed to find interferograms_found.txt file." 1>&2
  echo "{}"
  exit $STATUS
fi
if [ $TOTAL -ne 0 ]; then
  PROD_ID=`tail -1 interferograms_found.txt`
  echo "Interferogram was previously generated and exists in GRQ database with product ID $PROD_ID." 1>&2
  echo "{}"
  exit 0
fi

# download project specific DEM and config
ARIA_DEM_URL=`grep ARIA_DEM_URL= ${ARIAMH_HOME}/conf/settings.conf | cut -d= -f2`
ARIA_DEM_U=`grep ARIA_DEM_U= ${ARIAMH_HOME}/conf/settings.conf | cut -d= -f2`
ARIA_DEM_P=`grep ARIA_DEM_P= ${ARIAMH_HOME}/conf/settings.conf | cut -d= -f2`
if [ "$project" == "kilauea" ]; then
  echo "##########################################" 1>&2
  echo -n "Staging dem xml and dem for $project project: " 1>&2
  date 1>&2

  # download dem xml
  ARIA_DEM_XML=https://aria-alt-dav.jpl.nasa.gov/repository/products/kilauea/dem_kilauea.dem.xml
  curl --user ${ARIA_DEM_U}:${ARIA_DEM_P} -Ok $ARIA_DEM_XML > download_dem_xml.log 2>&1
  STATUS=$?
  echo -n "Finished downloading dem_${project}.dem.xml: " 1>&2
  date 1>&2
  if [ $STATUS -ne 0 ]; then
    echo -n "Failed to download dem_${project}.dem.xml: " 1>&2
    cat download_dem_xml.log 1>&2
    echo "{}"
    exit $STATUS
  fi

  # download dem
  ARIA_DEM_FILE=https://aria-alt-dav.jpl.nasa.gov/repository/products/kilauea/dem_kilauea.dem
  curl --user ${ARIA_DEM_U}:${ARIA_DEM_P} -Ok $ARIA_DEM_FILE > download_dem.log 2>&1
  STATUS=$?
  echo -n "Finished downloading dem_${project}.dem: " 1>&2
  date 1>&2
  if [ $STATUS -ne 0 ]; then
    echo -n "Failed to download dem_${project}.dem: " 1>&2
    cat download_dem.log 1>&2
    echo "{}"
    exit $STATUS
  fi

  echo -n "Finished staging dem and dem.xml for $project project: " 1>&2
  date 1>&2

  echo "##########################################" 1>&2
  echo -n "Write DEM input to job description JSON: " 1>&2
  date 1>&2
  /usr/bin/python3 $UTILS_HOME/jobDescriptorWriter.py --file $jobdesc_file \
    --set demFile "\"dem_kilauea.dem.xml\"" > jobDescriptorWriter.log 2>&1
  STATUS=$?
  echo -n "Finished writing DEM input to job description JSON: " 1>&2
  date 1>&2
  if [ $STATUS -lt 0 ]; then
    echo "Failed to write DEM input to job description JSON." 1>&2
    cat jobDescriptorWriter.log 1>&2
    echo "{}"
    exit $STATUS
  fi
fi

echo "##########################################" 1>&2
echo -n "Started interferogram creation: " 1>&2
date 1>&2
/usr/bin/python3 $INTERFEROGRAM_HOME/createInterferogram.py $jobdesc_file > createInterferogram.log 2>&1
STATUS=$?
echo -n "Finished interferogram creation: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to create interferogram." 1>&2
  cat createInterferogram.log 1>&2
  echo "{}"
  exit $STATUS
fi

# get DEM info
ARIA_DEM_XML=`ls dem*.dem.xml`
ARIA_DEM_FILE=`ls dem*.dem`

# get product dir
PROD=`ls -d interferogram_*`
STATUS=$?
if [ $STATUS -ne 0 ]; then
  echo "Failed to find interferogram product." 1>&2
  echo "{}"
  exit $STATUS
fi

# link browse images, add other metadata to *.met.json
echo "##########################################" 1>&2
echo -n "Soft-linking browse images and adding metadata: " 1>&2
date 1>&2
$INTERFEROGRAM_HOME/set_additional_metadata.sh $PROD $HASH > set_additional_metadata.log 2>&1
STATUS=$?
echo -n "Finished soft-linking browse images and adding metadata: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to create soft-links to browse images and add metadata." 1>&2
  cat set_additional_metadata.log 1>&2
  echo "{}"
  exit $STATUS
fi

# cleanup big files to save disk space
rm -rf *.h5 azimuthOffset.* catalog corrections_* dem* filt* insar.cpk lat.* \
  lon.* los.* output_* phsig.* rangeOffset.* resamp* simamp.* topophase.* z.* zsch.*

# for debugging use the following instead to cleanup
#mkdir debug_files
#mv -f *.ps *.png corrections_* debug_files/
#rm -rf *.h5 azimuthOffset.* catalog dem* filt* insar.cpk lat.* \
#    lon.* los.* output_* rangeOffset.* resamp* simamp.* z.* zsch.*

# generate GDAL (ENVI) header files and GeoTIFF products
cd $PROD
for i in `echo filt_topophase.flat filt_topophase.unw filt_topophase.unw.conncomp filt_topophase.unw.conncomp.geo filt_topophase.unw.geo los.rdr.geo phsig.cor.geo topophase.cor.geo`; do

  echo "##########################################" 1>&2
  echo -n "Generating GDAL (ENVI) header for $i: " 1>&2
  date 1>&2
  isce2gis.py envi -i $i >> ../isce2gis.log 2>&1
  echo -n "Finished generating GDAL (ENVI) header for $i: " 1>&2
  date 1>&2

  #echo "##########################################" 1>&2
  #echo -n "Generating GeoTIFF for $i: " 1>&2
  #date 1>&2
  #gdal_translate $i ${i}.tif >> ../gdal_translate.log 2>&1
  #echo -n "Finished generating GeoTIFF for $i: " 1>&2
  #date 1>&2

done
cd $WORK_DIR

# copy job description JSON to product
JOBDESC_PROD="${PROD}/${jobdesc_file}"
cp -f $jobdesc_file $JOBDESC_PROD

# write PROV-ES JSON
echo "##########################################" 1>&2
echo -n "Writing PROV-ES JSON: " 1>&2
date 1>&2
NETSEL_PROD="${PROD}/${netsel_file}"
$INTERFEROGRAM_HOME/create_prov_es-create_interferogram.sh $PROD $NETSEL_PROD $JOBDESC_PROD $project $ARIA_DEM_XML $ARIA_DEM_FILE $PROD $WORK_DIR $PROD/${PROD}.prov_es.json > create_prov_es.log 2>&1
STATUS=$?
echo -n "Finished writing PROV-ES JSON: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to write PROV-ES JSON." 1>&2
  cat create_prov_es.log 1>&2
  echo "{}"
  exit $STATUS
fi

exit 0
