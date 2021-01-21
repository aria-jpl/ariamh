FROM hysds/isce2:latest-es1
# to build GPU/CUDA version use this base instead:
#FROM hysds/isce2:latest-cuda

MAINTAINER aria-ops "aria-ops@list.jpl.nasa.gov"
LABEL description="GRFN PGE container"

# copying spyddderman, slcp2cor and slcp2pm repos
RUN git clone --branch python3 https://github.com/aria-jpl/slcp2pm.git $HOME/verdi/ops/slcp2pm \
    && git clone --branch python3 https://github.com/aria-jpl/slcp2cor.git $HOME/verdi/ops/slcp2cor \
    && sudo chown -R ops:ops $HOME/verdi/ops

# copy ariamh code ensure proper permissions, and move dependencies to final locations
COPY . /home/ops/ariamh

### RUN sudo yum -y update \
###     && sudo yum -y --setopt=tsflags=nodocs install unzip

RUN set -ex \
    && sudo /opt/conda/bin/conda install -c conda-forge --yes httplib2 lxml pyproj joblib pandas\
    && $HOME/verdi/bin/pip install joblib netcdf4 \
    &&  $HOME/verdi/bin/pip install 'fiona==1.7.13' \
    #&& $HOME/verdi/bin/pip install 'pyproj==2.6.1' \
    && sudo rm -rf /root/.cache \
    && sudo chown -R ops:ops /home/ops/ariamh \
    && cd /home/ops/verdi/ops/slcp2pm/src \
    && ./compile.sh \
    && cd /home/ops/verdi/ops/slcp2cor/src \
    && ./compile.sh

# set entrypoint
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
