USE idmapping;

set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_index;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping.jar;
create temporary function myfun as 'UDTFExplodeIDs';

INSERT OVERWRITE TABLE idmapping.index PARTITION (day='${hivevar:day}', product)
select
  id,
  collect_list(global_id)[0],
  product
from
(
  select
    a.id as id,
    global_id as global_id,
    a.pro as product
  from
    idmapping.ids lateral view myfun(
    map_keys(imei),
    map_keys(mac),
    map_keys(imsi),
    map_keys(phone_number),
    map_keys(idfa),
    map_keys(openudid),
    map_keys(uid),
    map_keys(did)
  ) a as id,pro
  where
    day='${hivevar:day}' and product='release'
) b
group by id,product
