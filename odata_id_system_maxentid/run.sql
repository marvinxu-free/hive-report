use odata;
ALTER TABLE id_system_maxentid  ADD  IF NOT EXISTS PARTITION (day='${day}', hour='${hour}') location  '/flume/event_maxentid/day=${day}/hour=${hour}'