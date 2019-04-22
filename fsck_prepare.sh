#!/bin/bash

# Author: Shashank Naik

export HADOOP_OPTS="-XX:-UseGCOverheadLimit $HADOOP_OPTS"
export HADOOP_CLIENT_OPTS="-Xmx20480m $HADOOP_CLIENT_OPTS"
DATE=`date +"%Y-%m-%d"`

hdfs_path_to_raw='/hdfs/path/to/raw/fsck/extract'
hdfs_path_to_final='/hdfs/path/to/processed/fsck/extract'

OUTFILE=fsck_runlog_$DATE.txt
echo 'starting run for date '$DATE >> ${OUTFILE}

hdfs fsck / -files -blocks -locations > fsck_raw_$DATE.out
grep "block(s):  OK" fsck_raw_$DATE.out > all_block_files.out

sed -i 's/,/\//g' all_block_files.out

# Select path, size, blocks
awk '{printf ("%s, %s, %s\n",$1,$(NF-4),$(NF-2))}' all_block_files.out > fsck_allBlockFiles_$DATE.csv

if $(hdfs dfs -test -d $hdfs_path_to_raw) ;
	then
	hdfs dfs -put fsck_raw_$DATE.out $hdfs_path_to_raw
else
 	hdfs dfs -mkdir -p $hdfs_path_to_raw >> ${OUTFILE} 2>&1
 	hdfs dfs -put fsck_raw_$DATE.out $hdfs_path_to_raw
fi

if $(hdfs dfs -test -d $hdfs_path_to_final) ;
	then
	hdfs dfs -put fsck_allBlockFiles_$DATE.csv $hdfs_path_to_final
else
 	hdfs dfs -mkdir -p $hdfs_path_to_final >> ${OUTFILE} 2>&1
 	hdfs dfs -put fsck_allBlockFiles_$DATE.csv $hdfs_path_to_final
fi

rm -f ./all_block_files.out
rm -f ./fsck_raw_$DATE.out
rm -f ./fsck_allBlockFiles_$DATE.csv

spark2-submit --executor-memory 6G --driver-memory 4G fsck_pyspark.py >> ${OUTFILE} 2>&1
