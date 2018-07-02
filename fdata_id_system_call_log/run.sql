add jar /services/apps/warehouse-manager-0.0.1-SNAPSHOT/lib/etl-1.0-SNAPSHOT.jar;
create temporary function query_info as "com.maxent.hive.udtf.QueryInfo";
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrick;
set hive.exec.max.created.files=655350;
set hive.exec.max.dynamic.partitions.pernode=655350;
set hive.exec.max.dynamic.partitions=655350;

use fdata;
insert overwrite table fdata.id_system_call_log partition(day, hour)
select
a.tid,
a.success,
a.fail,
b.*,
a.timestamp,
a.day,
a.hour
from
(select
get_json_object(json, '$.tid') as tid,
case when get_json_object(json,'$.responseBodyLength') >= 50 then 1 else 0 end as success,
case when get_json_object(json,'$.responseBodyLength') < 50 then 1 else 0 end as fail,
json,
get_json_object(json, '$.time') as timestamp,
day,
hour
--from_unixtime(   cast( (cast(   timestamp
--   as bigint )+28800000 )  /1000   as bigint)     ,'yyyyMMdd')  as `day`,
--from_unixtime(   cast( (cast(   timestamp
--   as bigint )+28800000 )  /1000   as bigint)     ,'HH') as `hour`
from odata.id_system_call_log
where day = '${day}') a lateral view query_info(get_json_object(json, '$.url')) b
