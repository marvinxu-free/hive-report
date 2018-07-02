set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrick;
set hive.exec.max.created.files=655350;
set hive.exec.max.dynamic.partitions.pernode=655350;
set hive.exec.max.dynamic.partitions=655350;
use adata;
create table IF NOT EXISTS id_system_call_log_statistics(
    tid string comment 'tid',
    success_rate double comment 'query success rate',
    success_rate1 double comment 'query success rate1 by id',
    `day` string comment 'date'
);

use adata;
insert overwrite table adata.id_system_call_log_statistics
select a.tid, a.success_rate,
b.success_id_counts / (b.success_id_counts + c.fail_id_counts) as success_rate1,
a.day
from
(select day, tid, sum(success) / (sum(success) + sum(fail)) as success_rate
from fdata.id_system_call_log
group by day, tid) a
join
(select day, tid, count(distinct query_id) as success_id_counts from fdata.id_system_call_log where success = 1
group by day, tid) b
on a.day = b.day and a.tid = b.tid
join (select day, tid, count(distinct query_id) as fail_id_counts from fdata.id_system_call_log where fail = 1
group by day, tid) c
on b.day = c.day and b.tid = c.tid;
