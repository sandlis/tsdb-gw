#!/bin/bash

set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/..

export GITVERSION=`git describe --always`

docker build -f scripts/Dockerfile -t raintank/tsdb-gw:$GITVERSION .
docker tag raintank/tsdb-gw:$GITVERSION raintank/tsdb-gw:latest
