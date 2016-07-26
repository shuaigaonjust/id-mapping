set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_vc_up;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
insert overwrite table idmapping.ids_2 partition (product="vc_up", day='20160630')
select
  distinct
  "" as global_id,
  myfun(regexp_extract(imei, "^([0-9a-zA-Z]{14,20})$", 1)) as imei,
  myfunmac(mac, 'mac') as mac,
  myfun(regexp_extract(imsi,"^([0-9]{15,16})$", 1)) as imsi,
  myfun("") as phone_number,
  myfun(regexp_extract(idfa,"^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$", 1)) as idfa,
  myfun(regexp_extract(openudid,"^([0-9a-zA-Z]{32,40})$", 1)) as openudid,
  myfun("") as uid,
  myfun(did) as did,
  myfun("") as android_id
from
  etl.vc_up
where
  day_time=date('2016-06-30')
;
