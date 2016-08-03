USE idmapping;
set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_original;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping_UDF.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
create temporary function myfunact as 'UDFIncreaseActivity';
insert overwrite table idmapping.ids_2 partition (product='original', day='20160529')
 select
 global_id,
 myfunact(imei),
 myfunact(mac),
 myfunact(imsi),
 myfunact(phone_number),
 myfunact(idfa),
 myfunact(openudid),
 myfunact(uid),
 myfunact(did),
 myfunact(android_id)
 from idmapping.ids_2
 where day="20160516"
 and product="release"
union all
 select
 global_id,
 imei,
 mac,
 imsi,
 phone_number,
 idfa,
 openudid,
 uid,
 did,
 android_id
 from idmapping.ids_2
 where day="20160523"
 and product="originalraw"
;
 

 USE idmapping;
 
 

 

 
set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_original;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping_UDF.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
create temporary function myfunact as 'UDFIncreaseActivity';

insert overwrite table idmapping.ids_2 partition (product='original', day='20160709')
select
global_id,
myfunact(imei),
myfunact(mac),
myfunact(imsi),
myfunact(phone_number),
myfunact(idfa),
myfunact(openudid),
myfunact(uid),
myfunact(did),
myfunact(android_id)
from idmapping.ids_2
where day="20160702"
and product="original"
union all
select
global_id,
imei,
mac,
imsi,
phone_number,
idfa,
openudid,
uid,
did,
myfun(map_keys(android_id)[0]) as android_id
from idmapping.ids_2
where day="201600709"
and product="original"
;