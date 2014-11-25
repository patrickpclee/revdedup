# RevDedup

## Welcome

This is the source code for RevDedup described in our paper presented in TOS.
The system is tested on Ubuntu 12.04 64 bit.  - July 2014

## Setup

The program can be compiled using Linux make. There are three required libraries
that users need to download separately. Brackets denote the package names in
Debian and Ubuntu platforms. Users can use apt-get to install the required
libraries.

	openssl (libssl-dev)
	libcurl (libcurl4-openssl-dev)
	Kyoto Cabinet (libkyotocabinet-dev)

## Storage Structure
	data
	 |--/bucket
	 |--/image

## Usage

### Chunking
Perform chunking on input file (Segment and Chunk) and output a metadata file

	./chunking <input file> <output chunking metafile>

### Inline Deduplication
Perform segment level deduplication and store the file

	./convdedup <input file> <input chunking metafile> <instanceID>

### Out-of-line Reverse Deduplication
Perform batch reverse deduplication for all instances at certain version

	./revdedup <number of instances> <version number>

### Restore Backup
Restore backups have undergone reverse deduplication (Reference Chain)

	./restoreo <instanceID> <version number> <output file>

Restore backups have undergone inline deduplication only
	./restore <instanceID> <version number> <output file>

### Delete Backup
Delete backups have undergone reverse deduplication (Fast Deletion)

	./deleteo <number of instances> <version number>

Perform deletion with conventional deduplication (Mark & Sweep)
	./remove <instanceID> <version number>
	./delete

## Usage Example
Assuming there are three versions of one VM image series, named vm0-0, vm0-1, vm0-2

#### 1. Chunking ####
	./chunking vm0-0 meta0-0
  	./chunking vm0-1 meta0-1
  	./chunking vm0-2 meta0-2
#### 2. Segment Level Deduplication ####
	./convdedup vm0-0 meta0-0 0
	./convdedup vm0-1 meta0-1 0
	./convdedup vm0-2 meta0-2 0
#### 3. Chunk Level Reverse Deduplication ####
	./revdedup 1 0
	./revdedup 1 1
#### 4. Restore ####
	./restoreo 0 0 restore0-0
	./restoreo 0 1 restore0-1
	./restore 0 2 restore0-2
#### 5. Delete ####
	./deleteo 1 0
	./deleteo 1 1
	./remove 0 2; ./delete

## Configuration
Parameters are set in include/revdedup.h
Queue Length (Memory Usage) are set in include/queue.h
