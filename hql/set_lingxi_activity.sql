set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_lingxi_activity;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
add jar ${env:HIVE_HOME}/lib/hive-hcatalog-core-2.0.0.jar;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
--alter table idmapping.lingxi add partition (day="20160721") location 'hdfs:///user/ylzhang5/datacopy/mi/lingxi/imsi/2016-07-07/';	
insert overwrite table idmapping.ids_2 partition (product="lingxi", day="20160707")
select distinct
global_id,
imei,
mac,
map(map_keys(imsi)[0],60),
map(map_keys(phone_number)[0],60),
idfa,
openudid,
uid,
did,
android_id
--  day
from
  idmapping.ids_2
where
day="20160721" and
product="lingxi"
;