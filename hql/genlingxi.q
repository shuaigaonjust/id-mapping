set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_lingxi;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
add jar ${env:HIVE_HOME}/lib/hive-hcatalog-core-2.0.0.jar;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
alter table idmapping.lingxi add partition (day="20160721") location 'hdfs:///user/ylzhang5/datacopy/mi/lingxi/imsi/2016-07-21/';	
insert overwrite table idmapping.ids_2 partition (product="lingxi", day="20160721")
select
  distinct
  "" as global_id,
  myfun("") as imei,
  myfun("") as mac,
  myfun(regexp_extract(imsi,"^([0-9]{15,16})$", 1)) as imsi,
  myfun(regexp_extract(Caller, "^([0-9]{11})$", 1)) as phone_number,
  myfun("") as idfa,
  myfun("") as openudid,
  myfun("") as uid,
  myfun("") as did,
  myfun("") as android_id
--  day
from
  idmapping.lingxi
where
day="20160721"
;