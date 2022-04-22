create table if not exists media.subscriber_info (
  subscriber_id string,
  msisdn string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
