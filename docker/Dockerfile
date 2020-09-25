FROM nisar/isce
# FROM hysds/isce2:latest-cuda
# FROM hysds/isce2:latest-cuda-es1


MAINTAINER aria-ops "aria-ops@list.jpl.nasa.gov"
LABEL description="GRFN PGE container"

# RUN sudo yum install -y git
RUN sudo yum install -y gcc-c++

# copying spyddderman, slcp2cor and slcp2pm repos
# RUN git clone --branch python3 https://github.com/aria-jpl/slcp2pm.git $HOME/verdi/ops/slcp2pm \
#     && git clone --branch python3 https://github.com/aria-jpl/slcp2cor.git $HOME/verdi/ops/slcp2cor \
#     && sudo chown -R ops:ops $HOME/verdi/ops

RUN set -ex \
 && . /opt/conda/bin/activate root \
 && sudo conda install --yes \
      cython \
      gdal \
      git \
      h5py \
      libgdal \
      pytest \
      numpy \
      fftw \
      scipy \
      scons \
      hdf4 \
      hdf5 \
      libgcc \
      libstdcxx-ng \
      cmake
      
RUN set -ex \
  && . /opt/conda/bin/activate root \
  && git clone --branch python3 https://github.com/aria-jpl/slcp2pm.git $HOME/verdi/ops/slcp2pm \
  && git clone --branch python3 https://github.com/aria-jpl/slcp2cor.git $HOME/verdi/ops/slcp2cor \
  && sudo chown -R conda $HOME/verdi/ops 


# RUN git clone --branch python3 https://github.com/aria-jpl/slcp2pm.git $HOME/verdi/ops/slcp2pm \
#    && git clone --branch python3 https://github.com/aria-jpl/slcp2cor.git $HOME/verdi/ops/slcp2cor \
#    && sudo chown -R conda $HOME/verdi/ops 




# copy ariamh code ensure proper permissions, and move dependencies to final locations
# COPY . /home/ops/ariamh
COPY . /home/conda/ariamh

# RUN set -ex \
#     && sudo /opt/conda/bin/conda install -c conda-forge --yes httplib2 lxml pyproj \
#     && $HOME/verdi/bin/pip install joblib netcdf4 \
#     &&  $HOME/verdi/bin/pip install 'fiona==1.7.13' \
    #&& $HOME/verdi/bin/pip install 'pyproj==2.6.1' \
#     && sudo rm -rf /root/.cache \
#     && sudo chown -R ops:ops /home/ops/ariamh \
#     && cd /home/ops/verdi/ops/slcp2pm/src \
#     && ./compile.sh \
#     && cd /home/ops/verdi/ops/slcp2cor/src \
#     && ./compile.sh

RUN set -ex \
    && sudo /opt/conda/bin/conda install -c conda-forge --yes httplib2 lxml pyproj \
    && /opt/conda/bin/pip install joblib netcdf4 \
    # && /opt/conda/bin/pip install 'fiona==1.7.13' \
    && sudo /opt/conda/bin/conda install -c conda-forge fiona \
    # && /opt/conda/bin/pip install 'pyproj==2.6.1' \
    && sudo /opt/conda/bin/conda install -c conda-forge pyproj \
    && sudo rm -rf /root/.cache \
    # && sudo chown -R conda /home/ops/ariamh \
    && sudo chown -R conda /home/conda/ariamh \
    # && cd /home/ops/verdi/ops/slcp2pm/src \
    && cd /home/conda/verdi/ops/slcp2pm/src \
    && ./compile.sh \
    # && cd /home/ops/verdi/ops/slcp2cor/src \
    && cd /home/conda/verdi/ops/slcp2cor/src \
    && ./compile.sh


# set entrypoint
# WORKDIR /home/ops
WORKDIR /home/conda
CMD ["/bin/bash", "--login"]
