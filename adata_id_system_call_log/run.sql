set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrick;
set hive.exec.max.created.files=655350;
set hive.exec.max.dynamic.partitions.pernode=655350;
set hive.exec.max.dynamic.partitions=655350;

use adata;
insert overwrite table adata.id_system_call_log partition(beijing_day, beijing_hour)
select a.tid,
a.num,
a.beijing_day,
a.beijing_hour
from(
select tid,
count(*) as num,
from_unixtime(   cast( (cast(   timestamp
   as bigint )+28800000 )  /1000   as bigint)     ,'yyyyMMdd')   as beijing_day,
from_unixtime(   cast( (cast(   timestamp
   as bigint )+28800000 )  /1000   as bigint)     ,'HH') as beijing_hour
from fdata.id_system_call_log
where day = '${day}'
group by tid,
from_unixtime(   cast( (cast(   timestamp
   as bigint )+28800000 )  /1000   as bigint)     ,'yyyyMMdd'),
from_unixtime(   cast( (cast(   timestamp
   as bigint )+28800000 )  /1000   as bigint)     ,'HH')
) a;