set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_ad_log;
set hive.exec.compress.output=true;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
--insert overwrite table idmapping.ids_2 partition (product="ad_log", day="${hivevar:year}${hivevar:month}${hivevar:day}")
insert overwrite table idmapping.ids_2 partition (product="ad_log", day="20160524")
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
--where day_time = date("${hivevar:year}-${hivevar:month}-${hivevar:day}");
where day_time = date("2016-05-24");

-- hdfs://ns-hf/user/compass/extract/ADDataClean/2016-05-24/orc
-- {"screenSize":"1.7","appTag":"杞︽湇鍔,"dspName":"EMPTY","appname":"58杩濈珷鏌ヨ","valong":-1,"resolution":"320*480","manufact":"apple","mediaType":"37","city":"娼嶅潑甯,"osType":"ios","timestamp":1.464037048541E12,"adCategoryID":"5","adCategory":"浼樻儬","dvcModel":"iphone 4s","adId":"ff9c5fa8809202180419c6358a779a07","manufactLable":"鑻规灉","ntt":"WIFI","isClick":"0","province":"灞变笢鐪,"mediaId":"3707","appTagID":"10271","dvc":"idfa_00256117-8999-4a0e-a469-821b20c03e44","valat":-1,"did":"EMPTY","operator":"绉诲姩","adShowType":"FOCUS_IMAGE","osVersion":"7","isExposure":"1","imei":"EMPTY","mac":"EMPTY","idfa":"00256117-8999-4a0e-a469-821b20c03e44","openudid":"EMPTY","clientIp":"60.210.197.108","dvcModelLable":"鑻规灉iPhone4S","dspId":"28"}

set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_ad_log;
set hive.exec.compress.output=true;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping_udf.jar;
create temporary function myfun as 'UDFGenMapStringInt';
create temporary function myfunmac as 'UDFGenMacMap';
insert overwrite table idmapping.ids_2 partition (product="ad_log", day="20160527")
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
  myfun("") as android_id
from 
  test.ad_log 
where day_time="20160527";

