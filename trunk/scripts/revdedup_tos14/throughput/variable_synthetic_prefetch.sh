#!/bin/bash

######## Evaluate Conv and Revdedup when posix_fadvise is in POSIX_WILLNEED mode

conv="/home/ldfs/min/revdedup_2.0_test/branches/conv_2.0"
rev="/home/ldfs/min/revdedup_2.0_test/branches/2.0"
data="/2year"
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
echo "Testing container prefetch with dataset $data_set"

#test revdedup on bucket size
cd /home/ldfs/min/rev2.0_test
mkdir -p /home/ldfs/min/rev2.0_test/rev_synthetic_$ds
mkdir -p /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/prefetch
for bs in ${bsize[@]}; do
cd $rev$bs
./prepare.sh

#initialize segment size and pf mode
sed -i.bak "s/\#define AVG_SEG_BLOCKS/\#define AVG_SEG_BLOCKS 1024 \/\//g" include/revdedup.h
sed -i.bak "s/\#define PF_MODE/\#define PF_MODE 1 \/\//g" include/revdedup.h
sed -i.bak "s/^\/\/ \#define PREFETCH_WHOLE_BUCKET/\#define PREFETCH_WHOLE_BUCKET /g" include/revdedup.h

make clean
make

./server & 
p=`ps aux | grep server | grep -v grep | awk '{print $2}'`

echo "copying chunking metadata..."
cp -r /home/ldfs/data/2year/2year7/meta_7_4K_4M $data_set/
echo "done"


sleep 5
for ((ver=0;ver<$vers;ver++)); do
	for ((i=0;i<$inst;i++)); do
		dropcache
		duration=`./convdedup $raw_data $data_set/meta_${ds}_4K_4M/inst$i$ver.meta $i | awk '{print $1}'`	
		echo "vm$(echo "$i+1"|bc)-$ver: $duration" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/prefetch/rev_write$bs.log

	done
	t0=$(date '+%s.%N')
	if [ $ver -gt ${retention[0]} ]; then
		./revdedup $inst $(echo "$ver-1-${retention[0]}"|bc)
	fi
	t1=$(date '+%s.%N')
	echo "vm$(echo "$i+1"|bc)-$ver: $(echo "$t1-$t0"|bc)" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/prefetch/rev_overhead$bs.log
	rm -rf inst*
	
done

for ((j=0;j<$(echo "$vers-1"|bc);j++)); do
	for ((i=0;i<$inst;i++)); do
		dropcache
		t0=$(date '+%s.%N')
		./restoreo $i $j /dev/null
		t1=$(date '+%s.%N')
		duration=$(echo "$t1-$t0" | bc)
		echo "VM$i-$j: $duration" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/prefetch/rev_read$bs.log
	done
done
for ((i=0;i<$inst;i++)); do
		dropcache
		t0=$(date '+%s.%N')
		./restore $i $(echo "$vers-1"|bc) /dev/null
		t1=$(date '+%s.%N')
		duration=$(echo "$t1-$t0" | bc)
		echo "VM$i-$(echo "$vers-1"|bc): $duration" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/prefetch/rev_read$bs.log
	done

kill $p
mv bucket_seeks.log /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/prefetch/rev_bucket_seeks$bs.log
./prepare.sh


rm -rf $data_set/meta_7_4K_4M


done

done
