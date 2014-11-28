#!/bin/bash

#
# Script to evaluate deduplication efficiency of conventional deduplication
# and reverse deduplication. The dataset used in this script is VM, course 
# backups of csci3150.
# 

conv="/home/ldfs/min/revdedup_2.0_test/branches/conv_2.0"
rev="/home/ldfs/min/revdedup_2.0_test/branches/2.0"
data="/home/ldfs/data/vm"
raw_data="$rev_4M/data/raw_image"
buckets="/home/ldfs/data/rev2.0/bucket/"
metadata="/home/ldfs/data/rev2.0/data/"

datasets=('hash')
retention=(0)
inst=160
vers=('1021' '1031' '1108' '1115' '1121' '1128' '1205' '1212' '1219' '1228' '1303' '1309')
bsize=('_4M' '_16M' '_32M')
ssize=('256' '1024' '2048')
conv_size=('_4K' '_1M' '_4M' '_8M')

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${rev}_4M/lib/:${conv}_4K/lib/

for ds in ${datasets[@]}; do
data=$data$ds
echo "Testing with datasets: $data"
cd /home/ldfs/min/rev2.0_test/
mkdir eval_storage_$ds


for ((run=0;run<$runs;run++)); do
#Conventional dedup baseline, 4KB segment, 16MB bucket
mkdir eval_storage_$ds
for cs in ${conv_size[@]}; do
cd "$conv$cs"
./prepare.sh
./server_meta &
p=`ps aux | grep server_meta | grep -v grep | awk '{print $2}'`

for ver in ${vers[@]}; do
	for ((i=0;i<$inst;i++)); do
		./chunking $data/vm$(echo "$i+1"|bc)-$ver inst$i.meta
		new=`./client_meta $raw_data inst$i.meta $i | grep 'New Segments:' | awk '{print $3}'`
		echo "$ver, $new" >> /home/ldfs/min/rev2.0_test/eval_storage_$ds/global$cs.log
	done
	rm -rf inst*
done
kill $p
./prepare.sh
sleep 5
done
done

#Test revdedup on segment size
for ss in ${ssize[@]}; do
cd $rev${bsize[2]}
./prepare.sh

#configure segment size
sed -i.bak "s/\#define AVG_SEG_BLOCKS/\#define AVG_SEG_BLOCKS $ss \/\//g" include/revdedup.h
sed -i.bak "s/\#define PF_MODE/\#define PF_MODE 2 \/\//g" include/revdedup.h
sed -i.bak "s/^\#define PREFETCH_WHOLE_BUCKET/\/\/ \#define PREFETCH_WHOLE_BUCKET /g" include/revdedup.h

make clean
make

./server_meta & 
p=`ps aux | grep server_meta | grep -v grep | awk '{print $2}'`
sleep 5
chks=0
new=0
for ((j=0;j<${#vers[@]};j++)); do
	for ((i=0;i<$inst;i++)); do
		./chunking $data/vm$(echo "$i+1"|bc)-${vers[j]} inst$i.meta
		new=`./client_meta $raw_data inst$i.meta $i | grep 'New Segments:' | awk '{print $3}'`	
echo "$j, $new, $chks" >> /home/ldfs/min/rev2.0_test/eval_storage_$ds/reverse_dedup_$ss.log
	done
	if [ $j -gt ${retention[0]} ]; then
		chks=`./revdedup_meta $inst $(echo "$j-1-${retention[0]}"|bc) | grep 'Global' | awk '{print $3}'`
	fi
	rm -rf inst*
	
done
kill $p
./prepare.sh
done
done
