create table if not exists mediatv_dds.subscriber_info (
  subscriber_id string,
  msisdn string,
  active_flg tinyint
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
