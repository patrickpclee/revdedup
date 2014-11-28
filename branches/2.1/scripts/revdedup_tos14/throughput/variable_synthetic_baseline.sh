#!/bin/bash

conv="/home/ldfs/min/revdedup_2.0_test/branches/conv_2.0"
rev="/home/ldfs/min/revdedup_2.0_test/branches/2.0"
data="/2year"
raw_data="$rev_4M/data/raw_image"
buckets="/home/ldfs/storage/bucket/"
metadata="/home/ldfs/storage/"

datasets=$1
retention=(0)
deletion_vers=(4 64)
inst=$2
vers=$3
size=('4M' '8M' '16M' '32M')
bsize=('_4M' '_16M' '_32M')
ssize=('256' '1024' '2048')
conv_size=('_4K')
runs=1

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${rev}_4M/lib/:${conv}_4K/lib/

for ds in ${datasets[@]}; do
data_set=$data$ds
echo "Testing baseline with dataset $data_set"


#Conventional dedup baseline, 4KB segment, 16MB bucket
cd /home/ldfs/min/rev2.0_test/
mkdir conv_synthetic_$ds
for cs in ${conv_size[@]}; do
cd "$conv$cs"
./prepare.sh
./server &
p=`ps aux | grep server | grep -v grep | awk '{print $2}'`

echo "Copying chunking metadata..."
cp -r /home/ldfs/data/2year/2year7/meta_7_4K_4K $data_set/
echo "done"


for ((ver=0;ver<$vers;ver++)); do
	for ((i=0;i<$inst;i++)); do
		duration=`./convdedup $raw_data $data_set/meta_${ds}_4K$cs/inst${i}${ver}.meta $i | awk '{print $1}'`
		echo "VM$(echo "$i+1"|bc)-$ver: $duration, $avg_seg" >> /home/ldfs/min/rev2.0_test/conv_synthetic_$ds/conv_write$cs.log
	done
	rm -rf inst*
done
bucket_size=`du -s data/bucket/ | awk '{print $1}'`
echo "$bucket_size" >> /home/ldfs/min/rev2.0_test/conv_synthetic_$ds/conv_space$cs.log

for ((j=0;j<$vers;j++)); do
	for ((i=0;i<$inst;i++)); do
		dropcache
		t0=$(date '+%s.%N')
		./restore $i $j /dev/null
		t1=$(date '+%s.%N')
		duration=$(echo "$t1-$t0" | bc)
		echo "VM$i-$j: $duration" >> /home/ldfs/min/rev2.0_test/conv_synthetic_$ds/conv_prefetch_read$cs.log
	done
done

kill $p
./prepare.sh
rm -rf $data_set/meta_7_4K_4K
sleep 5
done


done
