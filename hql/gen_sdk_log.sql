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