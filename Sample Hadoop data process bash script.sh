#!/bin/bash
# Load data to temporary table
hive -e "load data inpath '/dwh-data/installer-data/*' into table installer_temp;"
# Parse logs and insert them into staging table
hive -e "insert into table installer_staging
select
        regexp_extract(col_value, '(^[0-9.]*)') ip,
        from_unixtime(int(regexp_extract(col_value, '[0-9.]* ([0-9.]*)'))) server_timestamp,
        regexp_extract(col_value, 'sid=([0-9a-zA-Z\-]+)') session_id,
        regexp_extract(col_value, 'hid=([0-9a-zA-Z]+)') hw_id,
        regexp_extract(col_value, '&tr=([0-9]+)') tracker,
        regexp_extract(col_value, '&tr=[0-9]+-([A-Z0-9]+)-') country,
        regexp_extract(col_value, '&tr=[0-9]+-[A-Z0-9]+-([0-9]+)') distribution,
        regexp_extract(col_value, '&adm=([0-1]+)') admin,
        regexp_extract(col_value, '&os=([0-9.]+)') os,
        regexp_extract(col_value, '&x64=([0-9]+)') x64,
        regexp_extract(col_value, '&sil=([0-9]+)') silent,
        regexp_extract(col_value, '&e=([0-9]+)') event,
        regexp_extract(col_value, '&du=([0-9]+)') duration,
        regexp_extract(col_value, '&st=([0-9]+)') status
from installer_temp;"
# Copy parsed data from staging table to final table
hive -e "insert into table installer
select ip, server_timestamp, session_id, hw_id, tracker, country,
        distribution, admin, os, x64, silent, event, duration, status
from installer_staging;"
# Truncate temporary table
hive -e "truncate table installer_temp;"
# Truncate staging table
hive -e "truncate table installer_staging;"
