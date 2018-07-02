set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrick;
set hive.exec.max.created.files=655350;
set hive.exec.max.dynamic.partitions.pernode=655350;
set hive.exec.max.dynamic.partitions=655350;

use fdata;
insert overwrite table fdata.id_system_maxentid partition(day, hour)
select
get_json_object(json, '$.tid') as tid,
get_json_object(json, '$.pid') as pid,
get_json_object(json, '$.ref_id') as ref_id,
get_json_object(json, '$.sub_pid') as sub_pid,
get_json_object(json, '$.event_id') as event_id,
get_json_object(json, '$.maxent_id') as maxent_id,
get_json_object(json, '$.session_id') as session_id,
get_json_object(json, '$.ip') as ip,
get_json_object(json, '$.os') as os,
get_json_object(json, '$.device') as device,
get_json_object(json, '$.timestamp') as timestamp,
get_json_object(json, '$.host') as host,
get_json_object(json, '$.did') as did,
get_json_object(json, '$.did.imei') as imei,
get_json_object(json, '$.did.mac') as mac,
get_json_object(json, '$.did.aid') as aid,
get_json_object(json, '$.did.mcid') as mcid,
get_json_object(json, '$.did.idfa') as idfa,
get_json_object(json, '$.did.idfv') as idfv,
get_json_object(json, '$.did.mxcookie') as mxcookie,
get_json_object(json, '$.did.ckid') as ckid,
get_json_object(json, '$.user_agent') as user_agent,
get_json_object(json, '$.accept_charset') as accept_charset,
get_json_object(json, '$.accept_encoding') as accept_encoding,
get_json_object(json, '$.accept_language') as accept_language,
get_json_object(json, '$.sdkversion') as sdkversion,
get_json_object(json, '$.cr') as cr,
get_json_object(json, '$.ns') as ns,
get_json_object(json, '$.nssw') as nssw,
get_json_object(json, '$.type') as `type`,
get_json_object(json, '$.typeclass') as typeclass,
get_json_object(json, '$.message.__type') as inner_type,
get_json_object(json, '$.message.__api_key') as api_key,
get_json_object(json, '$.message.__user_id') as user_id,
get_json_object(json, '$.message.__session_id') as inner_session_id,
get_json_object(json, '$.message.__tact') as tact,
get_json_object(json, '$.message.__privileged') as client_privileged,
get_json_object(json, '$.message.__fields') as fields,
get_json_object(json, '$.message.__timestamp') as client_timestamp,
get_json_object(json, '$.campaign_id') as compaign_id,
get_json_object(json, '$.wscale') as wscale,
get_json_object(json, '$.tcpts') as tcpts,
get_json_object(json, '$.ttl') as ttl,
get_json_object(json, '$.tcp_source_port') as tcp_source_port,
get_json_object(json, '$.tcp_initial_window') as tcp_initial_window,
get_json_object(json, '$.package_id') as package_id,
get_json_object(json, '$.tick') as tick,
get_json_object(json, '$.privileged') as privileged,
get_json_object(json, '$.platform') as platform,
`day` as `day`,
`hour` as `hour`
from odata.id_system_maxentid
where day = '${day}';