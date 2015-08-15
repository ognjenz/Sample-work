#!/bin/bash
# ------------------------------------------------------------
# This script joins data from postsale and scr tables and 
# aggregates it per date per combo and stores it in a
# new table
# ------------------------------------------------------------
# Aggregate postsale data per date and combo
hive -e "add file /home/python-scripts/aggregate_ps_for_scr_udf_v2.py;
insert into table ps_aggregation
select transform (date(event_date), event, sale_amount*rate, recurring, order_id, 
                  case when combo='' then 'No combo' else combo end)
using 'python aggregate_ps_for_scr_udf_v2.py' as (date, combo, num_sales, 
                           num_refunds, num_cbks, avg_sale_amount, recurring_take_rate)
from postsale
where event in ('sale', 'refund', 'chargeback')
and date(event_date) = date_sub(from_unixtime(unix_timestamp()),1);"

# Aggregate scr data and join it with aggregated postsale data and insert into staging table
hive -e "insert into table ps_scr_staging
select t1.date date, t1.combo combo, t1.num hits, 
        case when t2.num_sales is null then 0 else t2.num_sales end as num_sales,
        case when t2.num_refunds is null then 0 else t2.num_refunds end as num_refunds,
        case when t2.num_cbks is null then 0 else t2.num_cbks end as num_cbks,
        case when t2.recurring_take_rate is null then 0 else t2.recurring_take_rate end as recurring_take_rate,
        case when t2.avg_sale_amount is null then 0 else t2.avg_sale_amount end as avg_sale_amount
from
(select cast(date(server_timestamp) as string) date, 
        case when combo='' then 'No combo' else combo end as combo, 
        count(distinct guid) num
from scr
where req_type = 'GET'
and bytes_sent > 0
and status = 200
and date(server_timestamp) = date_sub(from_unixtime(unix_timestamp()),1)
group by date(server_timestamp), combo) t1
left join
(select date, combo, num_sales, num_refunds, num_cbks, recurring_take_rate, avg_sale_amount
 from ps_aggregation) t2
on t1.date=t2.date and t1.combo=t2.combo;"

# Move data from staging table to ps_scr_aggregation
hive -e "insert into table ps_scr_aggregation
select date, combo, hits, num_sales, num_refunds, num_cbks, recurring_take_rate, avg_sale_amount
from ps_scr_staging
group by date, combo, hits, num_sales, num_refunds, num_cbks, recurring_take_rate, avg_sale_amount;"

# Truncate temporary table
hive -e "truncate table ps_aggregation;"
# Truncate staging table
hive -e "truncate table ps_scr_staging;"
