#!/bin/bash
set -x
BASE=$(dirname $0)
CODE_DIR=$(readlink -e "$BASE/../")

BUILD=$CODE_DIR/build

ARCH="$(uname -m)"
VERSION=$(git describe --long --always)

PACKAGE_NAME="${BUILD}/tsdb-gw-${VERSION}_${ARCH}.deb"
mkdir -p ${BUILD}/usr/bin
mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/etc/raintank

cp ${BASE}/etc/tsdb.ini ${BUILD}/etc/raintank/
mv ${BUILD}/tsdb-gw ${BUILD}/usr/bin/

fpm -s dir -t deb \
  -v ${VERSION} -n tsdb-gw -a ${ARCH} --description "HTTP gateway service for metrictank TSDB" \
  --deb-upstart ${BASE}/etc/init/tsdb-gw.conf \
  -C ${BUILD} -p ${PACKAGE_NAME} .

