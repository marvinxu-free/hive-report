set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrick;
set hive.exec.max.created.files=655350;
set hive.exec.max.dynamic.partitions.pernode=655350;
set hive.exec.max.dynamic.partitions=655350;

use adata;
insert overwrite table adata.id_system_events_by_maxentid partition(`day`, `hour`)
select a.tid, a.maxent_id, a.num, a.day, a.hour
from
(
select tid, day, hour, maxent_id, count(*) as num
from fdata.id_system_maxentid
where day = '${day}'
group by tid, day, hour, maxent_id
) a;
