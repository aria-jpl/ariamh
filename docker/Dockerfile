FROM hysds/isce_giant:latest

MAINTAINER aria-ops "aria-ops@list.jpl.nasa.gov"
LABEL description="GRFN PGE container"

USER root

# installing GCC compiler to update pyproj to 1.9.6
# installing Fortran ro run ./compile.sh in slcp2pm and slcp2cor
RUN yum -y install gcc \
    && yum -y install gcc-gfortran \
    && yum -y clean all \
    && /home/ops/verdi/bin/pip install 'pyproj==1.9.6'

USER ops

# copying spyddderman, slcp2cor and slcp2pm repos
RUN git clone --branch python3 https://github.com/aria-jpl/slcp2pm.git $HOME/verdi/ops/slcp2pm \
    && git clone --branch python3 https://github.com/aria-jpl/slcp2cor.git $HOME/verdi/ops/slcp2cor \
    && sudo chown -R ops:ops $HOME/verdi/ops

# copy ariamh code ensure proper permissions, and move dependencies to final locations
COPY . /home/ops/ariamh

RUN set -ex \
    && $HOME/conda/condabin/conda install gdal --yes \
    && $HOME/verdi/bin/pip install joblib netcdf4 \
    && sudo rm -rf /root/.cache \
    && sudo chown -R ops:ops /home/ops/ariamh \
    && cd /home/ops/verdi/ops/slcp2pm/src \
    && ./compile.sh \
    && cd /home/ops/verdi/ops/slcp2cor/src \
    && ./compile.sh

# set entrypoint
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
