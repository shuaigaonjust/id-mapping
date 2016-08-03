USE idmapping;
set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_original;
set hive.exec.compress.output=true;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping_udf.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';

insert overwrite table idmapping.ids_2 partition (product='original', day='20160523')
select distinct
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
where day>="201600523" and day<="20160529"
and product in ("ad_log", "lingxi", "mi_data", "py_yun", "sdk_log", "vcoam_log", "vc_up")
;