create schema if not exists landing;
create schema if not exists ods;
create schema if not exists dm;
create table if not exists landing.candles
(
  product_id varchar,
  time int,
  low numeric,
  high numeric,
  open numeric,
  close numeric,
  volume numeric,
  created_time timestamp
);
create table if not exists ods.candles
(
  product_id varchar,
  time int,
  low numeric,
  high numeric,
  open numeric,
  close numeric,
  volume numeric,
  created_time timestamp
);
create unique index if not exists ods_candles_idx on ods.candles(product_id, time);
create or replace view dm.candles
as
select product_id
  , time
  , to_timestamp(time) as ts
  , low
  , high
  , open
  , close
  , volume
from ods.candles;
