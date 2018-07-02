use odata;
ALTER TABLE id_system_call_log  ADD  IF NOT EXISTS PARTITION (day='${day}', hour='${hour}') location  '/flume/log_id_system_v2_call/day=${day}/hour=${hour}'