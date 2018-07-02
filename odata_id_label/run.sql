use odata;
create external table IF NOT EXISTS id_label
(
label string comment 'label',
browser string comment 'browser',
source_from string comment 'source(web or app)',
--test_company string comment 'test company',
event string comment 'event')
comment 'label data'
row format delimited
fields terminated by '\t'
location 'hdfs://hdnn/user/hive/warehouse/odata.db/id_label'