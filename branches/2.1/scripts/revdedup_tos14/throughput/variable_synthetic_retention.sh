#!/bin/bash

conv="/home/ldfs/min/revdedup_2.0_test/branches/conv_2.0"
rev="/home/ldfs/min/revdedup_2.0_test/branches/2.0"
data="/vmhash"
raw_data="$rev_4M/data/raw_image"
buckets="/home/ldfs/storage/data/bucket/"
metadata="/home/ldfs/storage/data/"

datasets=$1
retention=(0)
deletion_vers=(4 64)
inst=$2
vers=$3
size=('4M' '8M' '16M' '32M')
bsize=('_32M')
ssize=('256' '1024' '2048')
conv_size=('_4K')
runs=1

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${rev}_4M/lib/:${conv}_128K/lib/


for ds in ${datasets[@]}; do
data_set=$data$ds
echo "Testing with dataset $data_set"

#
# Revdedup retention window size
#
for ((run=0;run<$runs;run++)); do

#Test revdedup on retention window
mkdir -p /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/retention
for rt in ${retention[@]}; do
cd $rev${bsize[1]}
./prepare.sh

#configure segment size
sed -i.bak "s/\#define AVG_SEG_BLOCKS/\#define AVG_SEG_BLOCKS 1024 \/\//g" include/revdedup.h
sed -i.bak "s/\#define PF_MODE/\#define PF_MODE $4 \/\//g" include/revdedup.h
sed -i.bak "s/^\#define PREFETCH_WHOLE_BUCKET/\/\/ \#define PREFETCH_WHOLE_BUCKET /g" include/revdedup.h

make clean
make

./server & 
p=`ps aux | grep server | grep -v grep | awk '{print $2}'`
sleep 5
for ((ver=0;ver<$vers;ver++)); do
	for ((i=0;i<$inst;i++)); do
		./chunking $data_set/vm$(echo "$i+1"|bc)-$(echo "$ver+1"|bc) inst$i.meta
		dropcache
		duration=`./convdedup $raw_data inst$i.meta $i | awk '{print $1}'`	
		echo "VM$(echo "$i+1"|bc)-$ver: $duration" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/retention/rev_write_$rt.log
	done
	if [ $ver -gt $rt ]; then
		./revdedup $inst $(echo "$ver-1-$rt"|bc)
	fi
	rm -rf inst*
done

for ((j=0;j<$(echo "$vers-1-$rt"|bc);j++)); do
	for ((i=0;i<$inst;i++)); do
		dropcache
		t0=$(date '+%s.%N')
		./restoreo $i $j /dev/null
		t1=$(date '+%s.%N')
		duration=$(echo "$t1-$t0" | bc)
		echo "VM$i-$j: $duration" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/retention/rev_read_$rt.log
	done
done
for ((j=$(echo "$vers-1-$rt"|bc);j<$vers;j++)); do
	for ((i=0;i<$inst;i++)); do
		dropcache
		t0=$(date '+%s.%N')
		./restore $i $j /dev/null
		t1=$(date '+%s.%N')
		duration=$(echo "$t1-$t0" | bc)
		echo "VM$i-$j: $duration" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/retention/rev_read_$rt.log
	done
done
kill $p
mv bucket_seeks.log /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/retention/rev_bucket_seeks_$rt.log
./prepare.sh
done
done
done
