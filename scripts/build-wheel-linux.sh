#!/bin/bash

export VCPKG_ROOT=${PWD}
export PATH=\$VCPKG_ROOT:\$PATH

cd /acquire-zarr
/usr/bin/python3 -m build -o dist
/usr/bin/ls -l dist