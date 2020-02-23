#!/bin/bash

set -e

for D in `find examples/ -maxdepth 1 -mindepth 1 -type d`;
do
    pushd ${D}
    cargo build
    popd
done
