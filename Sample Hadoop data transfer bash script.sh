#!/bin/bash
# Lock procedure
mkdir .inst-lock
if [ $? -ne 0 ]; then exit 1; fi

# Copy files from shared drive to HDFS
hadoop fs -copyFromLocal /mnt/installer-logs/*.log /dwh-data/installer-data

# Compress processed files
bzip2 /mnt/installer-logs/*.log

# Move compressed files to backup location
mv /mnt/installer-logs/*.bz2 /mnt/backup-logs/installer-logs

# Delete lock folder
rm -rf .inst-lock
