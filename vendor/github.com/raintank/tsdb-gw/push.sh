#!/bin/bash
v=$(git describe)
echo "enter to continue pushing '$v'"
read
docker pull raintank/tsdb-gw:$v
docker tag raintank/tsdb-gw:$v us.gcr.io/kubernetes-dev/tsdb-gw:$v
gcloud docker -- push us.gcr.io/kubernetes-dev/tsdb-gw:$v
