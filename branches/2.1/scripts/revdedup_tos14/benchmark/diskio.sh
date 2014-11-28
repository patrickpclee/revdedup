#!/bin/bash

####### Disk IO benchmark using dd utility with fdatasync flag ########

num=20
wr=$1
bsizes=(16777216 33554432)

if [ $wr == 'w' ]; then
for ((i=0;i<$num;i++));do
	dropcache
	t0=$(date '+%s.%N')
	dd if=/run/shm/unique_image of=~/storage/unique_image bs=4M count=2K conv=fdatasync
	t1=$(date '+%s.%N')
	~/min/revdedup_2.0_test/branches/2.0_32M/prepare.sh
	#rm storage/unique_image
	echo $(echo "$t1-$t0"|bc) >> raid0_write
done
fi

if [ $wr == 'r' ]; then
	~/min/revdedup_2.0_test/branches/2.0_32M/prepare.sh
	cp /run/shm/unique_image ~/storage/
	dropcache
for ((i=0;i<$num;i++));do
	dropcache
	t0=$(date '+%s.%N')
	dd if=~/storage/unique_image of=/dev/null bs=4M
	t1=$(date '+%s.%N')
	echo $(echo "$t1-$t0"|bc) >> raid0_read
done
fi

if [ $wr == 'ww' ]; then
	for bs in ${bsizes[@]}; do
		umount storage
		mkfs.ext4 /dev/md1
		mount /dev/md1 storage
		mkdir storage/containers
		sleep 50
	for ((i=0;i<10;i++)); do
		duration=`./container_write /run/shm/unique_image $bs | awk '{print $2}'`
		echo $duration >> write_$bs
		#umount storage
		#mkfs.ext4 /dev/md1
		#mount /dev/md1 storage
		rm storage/containers/*
		sleep 1
	done
	done
fi
