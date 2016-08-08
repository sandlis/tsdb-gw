#!/bin/bash

set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

VERSION=`git describe --always`

mkdir build
cp ../build/tsdb-gw build/

docker build -t raintank/tsdb-gw:$VERSION .
docker tag raintank/tsdb-gw:$VERSION raintank/tsdb-gw:latest
