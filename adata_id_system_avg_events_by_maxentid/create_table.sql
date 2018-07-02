set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrick;
set hive.exec.max.created.files=655350;
set hive.exec.max.dynamic.partitions.pernode=655350;
set hive.exec.max.dynamic.partitions=655350;


use adata;
create table IF NOT EXISTS adata.id_system_avg_events_by_maxentid(
    `tid` string comment 'tid',
    `avg_num` double comment 'avg events per maxentid'
) PARTITIONED BY (`day` string comment 'date');;

use adata;
insert overwrite table adata.id_system_avg_events_by_maxentid partition(`day`)
select a.tid, a.total / a.maxentid_num as avg_num, a.day
from
(
select tid, day, count(distinct maxent_id) as maxentid_num, count(*) as total
from fdata.id_system_maxentid
where day >= 20170601
group by tid, day
) a;
