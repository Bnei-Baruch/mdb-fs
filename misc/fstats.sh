#!/usr/bin/env bash

SHA1=$(sha1sum $1 | awk '{print $1;}')
ABS=$(realpath $1)
SIZE=$(stat --printf="%s" $1)
MTIME=$(stat --printf="%Y" $1)

echo "[\"$ABS\",\"$SHA1\",$SIZE,$MTIME]"