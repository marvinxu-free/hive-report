set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrick;
set hive.exec.max.created.files=655350;
set hive.exec.max.dynamic.partitions.pernode=655350;
set hive.exec.max.dynamic.partitions=655350;
use adata;
-- 应该统计的是tracking server 收到的数据，目前tracking-server收到的数据没有落地，暂以id-system的出口的统计
use adata;
insert overwrite table adata.customer_upload partition(`beijing_day`, `beijing_hour`)
select a.tid,
a.pv,
a.did_uv,
a.maxent_id_uv,
a.beijing_day,
a.beijing_hour
from(
select tid,
count(*) as pv,
count(distinct concat_ws(mcid, idfa, idfv, aid, mxcookie, imei, mac, ckid)) as did_uv,
count(distinct maxent_id) as maxent_id_uv,
from_unixtime(   cast( (cast(   timestamp
   as bigint )+28800000 )  /1000   as bigint)     ,'yyyyMMdd')   as `beijing_day`,
from_unixtime(   cast( (cast(   timestamp
   as bigint )+28800000 )  /1000   as bigint)     ,'HH') as `beijing_hour`
from fdata.id_system_maxentid
where day = '${day}'
group by tid,
from_unixtime(   cast( (cast(   timestamp
   as bigint )+28800000 )  /1000   as bigint)     ,'yyyyMMdd'),
from_unixtime(   cast( (cast(   timestamp
   as bigint )+28800000 )  /1000   as bigint)     ,'HH')
) a;