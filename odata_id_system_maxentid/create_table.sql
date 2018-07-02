CREATE EXTERNAL TABLE IF NOT EXISTS `odata.id_system_maxentid`(
  `json` string)
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
  'hdfs://hdnn/flume/event_maxentid'
TBLPROPERTIES (
  'transient_lastDdlTime'='1462860280');

