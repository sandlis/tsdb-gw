#!/bin/bash -e

set -x

# GOPATH may have multiple paths, select the first one
cd $(sed 's/:.*//' <<< "$GOPATH")/src/github.com/raintank/tsdb-gw

GITVERSION=`git describe --always`

# Disable CGO for builds.
export CGO_ENABLED=0

# Make dir
mkdir -p build

# Clean build bin dir
rm -rf build/*

# Build binary
go build -ldflags "-X main.GitHash=$GITVERSION" -o build/tsdb-gw
