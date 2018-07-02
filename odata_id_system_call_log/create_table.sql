CREATE EXTERNAL TABLE IF NOT EXISTS odata.id_system_call_log(
  json string)
PARTITIONED BY (
  `day` string,
  `hour` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://hdnn/flume/log_id_system_v2_call'
TBLPROPERTIES (
  'transient_lastDdlTime'='1462860280');

