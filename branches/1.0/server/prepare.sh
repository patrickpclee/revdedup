#!/bin/bash

echo "db.dropDatabase()" | mongo rdserver
for (( i=0 ; i<256 ; i++ )); do
	x=`printf "%02x" $i`
	mkdir -p data/segment/$x
	mkdir -p data/blockfp/$x
	mkdir -p data/oldsegment/$x
done

mkdir -p data/vmap
