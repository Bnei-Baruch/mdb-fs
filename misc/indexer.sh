#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
find $1 -type f \( -path $1/index \) -prune -o -type f -exec $DIR/fstats.sh {} \; > $1/index