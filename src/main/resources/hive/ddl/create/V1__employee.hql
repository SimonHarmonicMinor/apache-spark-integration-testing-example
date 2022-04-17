create table if not exists employee (
  eid int,
  ename string,
  age int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
