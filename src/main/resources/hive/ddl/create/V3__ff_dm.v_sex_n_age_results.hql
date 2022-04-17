create table if not exists ff_dm.v_sex_n_age_results (
  msisdn string,
  app_n bigint,
  regid smallint,
  sex string,
  age string,
  source string,
  business_dt string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
