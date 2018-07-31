#!/usr/bin/env bash
set +e
set -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
find $1 ! -path "${1}/index" -type f -exec $DIR/fstats.sh {} \; > $1/index
