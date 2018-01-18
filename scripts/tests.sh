#!/bin/bash

set -xe

SCRIPTS_DIR=$(dirname ${BASH_SOURCE[0]})
SOURCE_DIR=$SCRIPTS_DIR/..
TMP_DIR=$(mktemp -d)

cd $SOURCE_DIR

if ! [ -d $PKG_CONFIG_PATH ] || [ -z $PKG_CONFIG_PATH ]
then
	source scripts/build_deps.sh
else
	echo "not building librdkafka"
fi

go test -tags static -v -race ./...
go vet ./...

# delete temporary build dir of librdkafka, since it is linked statically we
# don't need it anymore
rm -rf $TMP_DIR
