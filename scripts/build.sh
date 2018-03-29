#!/bin/bash -e

set -xe

SCRIPTS_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
SOURCE_DIR=$SCRIPTS_DIR/..
BUILD_DIR=$SOURCE_DIR/build
TMP_DIR=$(mktemp -d)

cd $SOURCE_DIR

if ! [ -d $PKG_CONFIG_PATH ] || [ -z $PKG_CONFIG_PATH ]
then
	source scripts/build_deps.sh
else
	echo "not building librdkafka"
fi

# get git version
GIT_VERSION=`git describe --always`

# Make dir
mkdir -p $BUILD_DIR

# Clean build bin dir
rm -rf $BUILD_DIR/*

# Build binary
cd cmd/tsdb-gw
CGO_ENABLED=1 go build -tags static -ldflags "-X main.GitHash=$GIT_VERSION" -o $BUILD_DIR/tsdb-gw
cd ../tsdb-usage
CGO_ENABLED=1 go build -tags static -ldflags "-X main.GitHash=$GIT_VERSION" -o $BUILD_DIR/tsdb-usage
cd ../cortex-gw
go build -tags static -ldflags "-X main.GitHash=$GIT_VERSION" -o $BUILD_DIR/cortex-gw

# delete temporary build dir of librdkafka, since it is linked statically we
# don't need it anymore
echo rm -rf $TMP_DIR
