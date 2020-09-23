FROM nisar/isce
# FROM hysds/isce2:latest-cuda
# FROM hysds/isce2:latest-cuda-es1


MAINTAINER aria-ops "aria-ops@list.jpl.nasa.gov"
LABEL description="GRFN PGE container"

USER ops

# copy ariamh code ensure proper permissions, and move dependencies to final locations
COPY . /home/ops/ariamh
RUN set -ex \
 && source ~/.bash_profile \
 && pip install joblib netcdf4 \
 && sudo rm -rf /root/.cache \
 && sudo chown -R ops:ops /home/ops/ariamh \
 && cd /home/ops/verdi/ops/ && git clone https://github.com/hysds/slcp2pm.git \
 && git clone https://github.com/hysds/slcp2cor.git \
 #&& git clone -b standard-product https://github.com/mkarim2017/spyddder-man.git \
 #&& mv /home/ops/ariamh/slcp2cor /home/ops/verdi/ops/ \
 && cd /home/ops/verdi/ops/slcp2pm/src \
 && ./compile.sh \
 && cd /home/ops/verdi/ops/slcp2cor/src \
 && ./compile.sh

# set entrypoint
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
