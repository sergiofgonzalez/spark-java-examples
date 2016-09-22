#!/bin/bash

if [ -z "$1" ];
then
  echo "Missing output directory name"
  echo "Usage: run-split-and-send.sh [hdfs-or-local-dir-name] [local]  where local is an optional argument to use local FS instead of HDFS"
  exit 1
fi

# Clear previous mess (if any)
rm -rf input-data/orders.txt
rm -rf input-data/$1/*.ordtmp

# Untarring the large input data file
tar xvzf input-data/orders.tar.gz -C input-data/

# Split into files of 10_000 lines with .ordtmp extension
split -l 10000 --additional-suffix=.ordtmp ./input-data/orders.txt ./input-data/orders

for f in $(ls input-data/*.ordtmp);
do
  if [ "$2" == "local" ];
  then
    echo "sending file (local) $1 -> $f"
    mv $f $1
  else
    echo "sending file (HDFS) $1 -> $f"
    hdfs dfs -copyFromLocal $f $1
    rm -f $f
  fi
  sleep 3
done

rm -rf input-data/$1
rm -rf input-data/orders.txt
