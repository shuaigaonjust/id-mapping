set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_sdk_log;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
insert overwrite table idmapping.ids_2 partition (product="sdk_log",day="${hivevar:year}${hivevar:month}${hivevar:day}")
select
  distinct
  "" as global_id,
  myfun(regexp_extract(imei,"^([0-9a-zA-Z]{14,20})$", 1)) as imei,
  myfunmac(mac, "mac") as mac,
  myfun(regexp_extract(imsi,"^([0-9]{15,16})$", 1)) as imsi,
  myfun("") as phone_number,
  myfun("") as idfa,
  myfun("") as openudid,
  myfun("") as uid,
  myfun(did) as did,
  myfun("") as android_id
from
  etl.sdk_log
where day_time = date("${hivevar:year}-${hivevar:month}-${hivevar:day}");


set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_vcoam;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
insert overwrite table idmapping.ids_2 partition (product="vcoam_log",day="${hivevar:year}${hivevar:month}${hivevar:day}")
--insert overwrite table idmapping.ids_2 partition (product="vcoam_log",day="20160516")
select
  distinct
  "" as global_id,
  myfun(regexp_extract(imei,"^([0-9a-zA-Z]{14,20})$", 1)) as imei,
  myfunmac(mac, "mac") as mac,
  myfun(regexp_extract(imsi,"^([0-9]{15,16})$", 1)) as imsi,
  myfun("") as phone_number,
  myfun(regexp_extract(idfa,"^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$", 1)) as idfa,
  myfun(regexp_extract(openudid,"^([0-9a-zA-Z]{32,40})$", 1)) as openudid,
  myfun("") as uid,
  myfun(did) as did,
  myfun(android_id) as android_id
from
  etl.vcoam_log
where day_time = date("${hivevar:year}-${hivevar:month}-${hivevar:day}");
--where day_time = date("2016-05-16");

set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_ids2;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
insert overwrite table idmapping.ids_2 partition (product="py_yun",day="${hivevar:year}${hivevar:month}${hivevar:day}")
--insert overwrite table idmapping.ids_2 partition (product="py_yun",day="20160516")
select
  distinct
  "" as global_id,
  myfun(case when split(dvc, '_')[0]='imei' then regexp_extract(split(dvc, '_')[1], "^([0-9a-zA-Z]{15})$", 1)else "" end) as imei,
  myfunmac(dvc, 'mac') as mac,
  myfun("") as imsi,
  myfun("") as phone_number,
  myfun("") as idfa,
  myfun("") as openudid,
  myfun(uid) as uid,
  myfun(did) as did,
  myfun("") as android_id
from
  etl.py_yun
where day_time = date("${hivevar:year}-${hivevar:month}-${hivevar:day}");
--where day_time = date("$2016-05-16");


set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_ids3;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
insert overwrite table idmapping.ids_2 partition (product="mi_data", day="${hivevar:year}${hivevar:month}${hivevar:day}")
--insert overwrite table idmapping.ids_2 partition (product="mi_data", day="20160516")
select distinct 
  "" as global_id,
  myfun(regexp_extract(dvc, "^([0-9a-zA-Z]{14,20})$", 1)) as imei,
  myfun("") as mac,
  myfun("") as imsi,
  myfun("") as phone_number,
  myfun("") as idfa,
  myfun("") as openudid,
  myfun("") as uid,
  myfun(did) as did,
  myfun("") as android_id
from 
  etl.mi_data 
where day_time = date("${hivevar:year}-${hivevar:month}-${hivevar:day}");
--where day_time = date("2016-05-16");

set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_ids4;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
insert overwrite table idmapping.ids_2 partition (product="ad_log", day="${hivevar:year}${hivevar:month}${hivevar:day}")
--insert overwrite table idmapping.ids_2 partition (product="ad_log", day="20160516")
select distinct 
  "" as global_id,
  myfun(regexp_extract(imei, "^([0-9a-zA-Z]{14,20})$", 1)) as imei,
  myfunmac(mac,'mac') as mac,
  myfun("") as imsi,
  myfun("") as phone_number,
  myfun(regexp_extract(idfa, "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$", 1)) as idfa,
  myfun(regexp_extract(openudid, "^([0-9a-zA-Z]{32,40})$", 1)) as openudid,
  myfun("") as uid,
  myfun(did) as did,
  myfun(android_id) as android_id
from 
  etl.ad_log 
where day_time = date("${hivevar:year}-${hivevar:month}-${hivevar:day}");
--where day_time = date("2016-05-16");
