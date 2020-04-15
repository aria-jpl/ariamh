FROM hysds/isce2:latest

MAINTAINER aria-ops "aria-ops@list.jpl.nasa.gov"
LABEL description="GRFN PGE container"


USER root

RUN /home/ops/verdi/bin/pip3 install 'pyproj==1.9.6'
RUN /home/ops/verdi/bin/pip install 'pyproj==1.9.6'

USER ops


# copy ariamh code ensure proper permissions, and move dependencies to final locations
COPY . /home/ops/ariamh
RUN set -ex \
 && sudo /home/ops/verdi/bin/pip3 install joblib netcdf4 \
 && sudo rm -rf /root/.cache \
 && sudo chown -R ops:ops /home/ops/ariamh \
 && mv /home/ops/ariamh/alos2-ingest /home/ops/verdi/ops/ 

# set entrypoint
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
