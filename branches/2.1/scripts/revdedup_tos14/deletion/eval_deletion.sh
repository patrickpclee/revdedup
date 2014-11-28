#!/bin/bash

conv="/home/ldfs/min/revdedup_2.0_test/branches/conv_2.0"
rev="/home/ldfs/min/revdedup_2.0_test/branches/2.0"
data="/vmhash"
raw_data="${rev}_4M/data/raw_image"
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
# conv deletion
#
for ((run=0;run<$runs;run++)); do

#Test conv deletion overhead
mkdir -p /home/ldfs/min/rev2.0_test/rev_synthetic/deletion
for dv in ${deletion_vers[@]}; do
cd $rev${bsize[1]}
./prepare.sh

#configure segment size
sed -i.bak "s/\#define AVG_SEG_BLOCKS/\#define AVG_SEG_BLOCKS 1 \/\//g" include/revdedup.h
sed -i.bak "s/\#define PF_MODE/\#define PF_MODE 2 \/\//g" include/revdedup.h
sed -i.bak "s/^\#define PREFETCH_WHOLE_BUCKET/\/\/ \#define PREFETCH_WHOLE_BUCKET /g" include/revdedup.h

make clean
make

./server & 
p=`ps aux | grep server | grep -v grep | awk '{print $2}'`
sleep 5
for ((ver=0;ver<$vers;ver++)); do
	for ((i=0;i<$inst;i++)); do
		./chunking $data/vm$(echo "$i+1"|bc)-$(echo "$ver+1"|bc) inst$i.meta
		duration=`./convdedup $raw_data inst$i.meta $i | awk '{print $1}'`	
	done
	rm -rf inst*
done

		dropcache
		t0=$(date '+%s.%N')
for ((j=0;j<$inst;j++)); do
	for ((i=0;i<$dv;i++)); do
		./remove $j $i
	done
done
		./delete
		t1=$(date '+%s.%N')
		duration=$(echo "$t1-$t0"|bc)
		echo "Delete1-$dv: $duration" >> /home/ldfs/min/rev2.0_test/rev_synthetic/deletion/rev_deletion_refcnt.log
kill $p
done
done


#
# revdedup deletion
#
for ((run=0;run<$runs;run++)); do

#Test revdedup deletion overhead
mkdir -p /home/ldfs/min/rev2.0_test/rev_synthetic/deletion
for dv in ${deletion_vers[@]}; do
cd $rev${bsize[1]}
./prepare.sh

#configure segment size
sed -i.bak "s/\#define AVG_SEG_BLOCKS/\#define AVG_SEG_BLOCKS 1 \/\//g" include/revdedup.h
sed -i.bak "s/\#define PF_MODE/\#define PF_MODE 2 \/\//g" include/revdedup.h
sed -i.bak "s/^\#define PREFETCH_WHOLE_BUCKET/\/\/ \#define PREFETCH_WHOLE_BUCKET /g" include/revdedup.h

make clean
make

./server & 
p=`ps aux | grep server | grep -v grep | awk '{print $2}'`
sleep 5
for ((ver=0;ver<$vers;ver++)); do
	for ((i=0;i<$inst;i++)); do
		./chunking $data/vm$(echo "$i+1"|bc)-$(echo "$ver+1"|bc) inst$i.meta
		duration=`./convdedup $raw_data inst$i.meta $i | awk '{print $1}'`	
	done
	if [ $ver -gt ${retention[0]} ]; then
		./revdedup $inst $(echo "$ver-1-${retention[0]}"|bc)
	fi
	rm -rf inst*
done

		dropcache
		t0=$(date '+%s.%N')
	#for ((i=0;i<$vers;i++)); do
		./deleteo $inst $(echo "$vers-2"|bc)
for ((i=0;i<$inst;i++)); do
		./remove $i $(echo "$vers-1"|bc)
done
		./delete
		t1=$(date '+%s.%N')
		duration=$(echo "$t1-$t0"|bc)
		echo "Delete1-$dv: $duration" >> /home/ldfs/min/rev2.0_test/rev_synthetic/deletion/rev_deletion_refcnt.log
kill $p
done
done


done
