#!/bin/bash

# clone spyddder-man to be moved to its final location by docker builder
git clone -b standard-product https://github.com/mkarim2017/spyddder-man.git

# clone slcp2pm to be moved to its final location by docker builder
git clone https://github.com/hysds/slcp2pm.git

# clone slcp2cor to be moved to its final location by docker builder
git clone https://github.com/hysds/slcp2cor.git
