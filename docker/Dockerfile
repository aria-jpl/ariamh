FROM hysds/pge-isce_giant

MAINTAINER aria-ops "aria-ops@list.jpl.nasa.gov"
LABEL description="GRFN PGE container"

ARG id
ARG gid

# create work directory
RUN set -ex \
 && mkdir -p /data/work \
 && chmod -R 755 /data \
 && chown -R ops:ops /data

# copy ariamh code ensure proper permissions, and move dependencies to final locations
COPY . /home/ops/ariamh
RUN set -ex \
 && sudo chown -R ops:ops /home/ops/ariamh \
 && mv /home/ops/ariamh/spyddder-man /home/ops/verdi/ops/ \
 && mv /home/ops/ariamh/slcp2pm /home/ops/verdi/ops/ \
 && cd /home/ops/verdi/ops/slcp2pm/src \
 && ./compile.sh

# set entrypoint
USER ops
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
