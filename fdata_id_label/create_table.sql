set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrick;
set hive.exec.max.created.files=655350;
set hive.exec.max.dynamic.partitions.pernode=655350;
set hive.exec.max.dynamic.partitions=655350;
use fdata;
CREATE  TABLE IF NOT EXISTs `id_label`(
  `label` string comment 'label',
  `source_from` string comment 'source_from',
  `browser` string comment 'browser',
    --test_company string comment 'test company',
  `tid` string comment 'app_id',
  `pid` string comment 'channel id',
  `ref_id` string comment 'ref_id',
  `sub_pid` string comment 'sub channel id',
  `event_id` string comment 'event id',
  `maxent_id` string comment 'maxent id',
  `session_id` string comment 'session id',
  `ip` string comment 'ip',
  `os` string comment 'operate system',
  `device` string comment 'device type',
  `timestamp` string comment 'timestamp',
  `host` string comment 'tracking server host ip',
  `did` string comment 'did',
  `imei` string comment 'imei',
  `mac` string comment 'mac',
  `aid` string comment 'android id',
  `mcid` string comment 'mcid',
  `idfa` string comment 'idfa',
  `idfv` string comment 'idfv',
  `mxcookie` string comment 'mxcookie',
  `user_agent` string comment 'user_agent',
  `accept_charset` string comment 'accept_charset',
  `accept_encoding` string comment 'accept_encoding',
  `accept_language` string comment 'accept_language',
  `sdkversion` string comment 'sdk version',
  `cr` string comment 'operator',
  `ns` string comment 'network provider',
  `nssw` string comment 'network provider',
  `type` string comment 'event type',
  `typeclass` string comment 'event class',
  `inner_type` string comment 'client event type',
  `api_key` string comment 'app id',
  `user_id` string comment 'user id',
  `inner_session_id` string comment 'client session id',
  `tact` string comment 'act event type',
  `client_privileged` string comment 'client is privileged or not',
  `fields` string comment 'fields',
  `client_timestamp` string comment 'client timestamp',
  `compaign_id` string comment 'compaign id',
  `wscale` string comment 'wscale',
  `tcpts` string comment 'tcpts',
  `ttl` string comment 'ttl',
  `tcp_source_port` string comment 'tcp_source_port',
  `tcp_initial_window` string comment 'tcp_initial_window',
  `package_id` string comment 'package_id',
  `tick` string comment 'tick',
  `privileged` string comment 'is privileged',
  `platform` string comment 'platform');


insert overwrite table fdata.id_label
select
label,
source_from,
browser,
get_json_object(event, '$.tid') as tid,
get_json_object(event, '$.pid') as pid,
get_json_object(event, '$.ref_id') as ref_id,
get_json_object(event, '$.sub_pid') as sub_pid,
get_json_object(event, '$.event_id') as event_id,
get_json_object(event, '$.maxent_id') as maxent_id,
get_json_object(event, '$.session_id') as session_id,
get_json_object(event, '$.ip') as ip,
get_json_object(event, '$.os') as os,
get_json_object(event, '$.device') as device,
get_json_object(event, '$.timestamp') as timestamp,
get_json_object(event, '$.host') as host,
get_json_object(event, '$.did') as did,
get_json_object(event, '$.did.imei') as imei,
get_json_object(event, '$.did.mac') as mac,
get_json_object(event, '$.did.aid') as aid,
get_json_object(event, '$.did.mcid') as mcid,
get_json_object(event, '$.did.idfa') as idfa,
get_json_object(event, '$.did.idfv') as idfv,
get_json_object(event, '$.did.mxcookie') as mxcookie,
get_json_object(event, '$.user_agent') as user_agent,
get_json_object(event, '$.accept_charset') as accept_charset,
get_json_object(event, '$.accept_encoding') as accept_encoding,
get_json_object(event, '$.accept_language') as accept_language,
get_json_object(event, '$.sdkversion') as sdkversion,
get_json_object(event, '$.cr') as cr,
get_json_object(event, '$.ns') as ns,
get_json_object(event, '$.nssw') as nssw,
get_json_object(event, '$.type') as `type`,
get_json_object(event, '$.typeclass') as typeclass,
get_json_object(event, '$.message.__type') as inner_type,
get_json_object(event, '$.message.__api_key') as api_key,
get_json_object(event, '$.message.__user_id') as user_id,
get_json_object(event, '$.message.__session_id') as inner_session_id,
get_json_object(event, '$.message.__tact') as tact,
get_json_object(event, '$.message.__privileged') as client_privileged,
get_json_object(event, '$.message.__fields') as fields,
get_json_object(event, '$.message.__timestamp') as client_timestamp,
get_json_object(event, '$.campaign_id') as compaign_id,
get_json_object(event, '$.wscale') as wscale,
get_json_object(event, '$.tcpts') as tcpts,
get_json_object(event, '$.ttl') as ttl,
get_json_object(event, '$.tcp_source_port') as tcp_source_port,
get_json_object(event, '$.tcp_initial_window') as tcp_initial_window,
get_json_object(event, '$.package_id') as package_id,
get_json_object(event, '$.tick') as tick,
get_json_object(event, '$.privileged') as privileged,
get_json_object(event, '$.platform') as platform
from odata.id_label
