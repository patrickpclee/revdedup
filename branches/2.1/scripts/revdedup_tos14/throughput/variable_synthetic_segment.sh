#!/bin/bash

#
# Evaluate conv and revdedup when segment size varies. Bucket size is fixed as
# 32M, prefetch mode is POSIX_DONTNEED and deduplication ratios before and after
# reverse deduplication are recorded
#

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
ssize=(256 2048 1024)
conv_size=('_4K')
runs=1

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${rev}_4M/lib/:${conv}_128K/lib/


for ds in ${datasets[@]}; do
data_set=$data$ds
echo "Testing segment size with dataset $data_set"


cd /home/ldfs/min/rev2.0_test
mkdir -p /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/segment
for ss in ${ssize[@]}; do
cd $rev${bsize[0]}
./prepare.sh

#configure segment size
sed -i.bak "s/\#define AVG_SEG_BLOCKS/\#define AVG_SEG_BLOCKS $ss \/\//g" include/revdedup.h
sed -i.bak "s/\#define PF_MODE/\#define PF_MODE 2 \/\//g" include/revdedup.h
sed -i.bak "s/^\#define PREFETCH_WHOLE_BUCKET/\/\/ \#define PREFETCH_WHOLE_BUCKET /g" include/revdedup.h

make clean
make

./server & 
p=`ps aux | grep server | grep -v grep | awk '{print $2}'`
sg_num=$(echo "$ss/256"|bc)
sg_num=$sg_num"M"
echo "Testing with $sg_num segment."

echo "copying chunking metadata..."
cp -r /home/ldfs/data/2year/2yearGP/meta_GP_4K_${sg_num} $data_set/
echo "done"



sleep 5
for ((ver=0;ver<$vers;ver++)); do
	for ((i=0;i<$inst;i++)); do
		dropcache
		duration=`./convdedup $raw_data $data_set/meta_${ds}_4K_${sg_num}/inst$(echo "$i+1"|bc)-${ver}.meta $i | awk '{print $1}'`	
		echo "VM$(echo "$i+1"|bc)-$ver: $duration, $avg_seg" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/segment/rev_write_$ss.log

	done
	rm -rf inst*
done

#### MEASURE Overall Disk Consumption w/o Reverse Deduplication #########
bucket_size=`du -s data/bucket/ | awk '{print $1}'`
echo "$bucket_size" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/segment/rev_global_$ss.log


#### Do Batch Reverse Deduplication #######
for ((ver=0;ver<$vers;ver++)); do
	t0=$(date '+%s.%N')
	if [ $ver -gt ${retention[0]} ]; then
		./revdedup $inst $(echo "$ver-1-${retention[0]}"|bc)
	fi
	t1=$(date '+%s.%N')
	echo "VM$(echo "$i+1"|bc)-$ver: $(echo "$t1-$t0"|bc)" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/segment/rev_overhead_$ss.log

done


#### MEASURE Overall Disk Consumption After Reverse Deduplication #########
bucket_size=`du -s data/bucket/ | awk '{print $1}'`
echo "$bucket_size" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/segment/rev_reverse_$ss.log

for ((j=0;j<$(echo "$vers-1"|bc);j++)); do
	for ((i=0;i<$inst;i++)); do
		dropcache
		t0=$(date '+%s.%N')
		./restoreo $i $j /dev/null
		t1=$(date '+%s.%N')
		duration=$(echo "$t1-$t0" | bc)
		echo "VM$i-$j: $duration" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/segment/rev_read_$ss.log
	done
done
	for ((i=0;i<$inst;i++)); do
		dropcache
		t0=$(date '+%s.%N')
		./restore $i $(echo "$vers-1"|bc) /dev/null
		t1=$(date '+%s.%N')
		duration=$(echo "$t1-$t0" | bc)
		echo "VM$i-$(echo "$vers-1"|bc): $duration" >> /home/ldfs/min/rev2.0_test/rev_synthetic_$ds/segment/rev_read_$ss.log
	done
kill $p
./prepare.sh

rm -rf $data_set/meta_GP_4K_${sg_num}

done


done
