use idmapping;
set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_gen_ids;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/hive-hcatalog-core-2.0.0.jar;
add jar hdfs://ns-hf/user/compass/public/hive/idmapping/jars/idmapping.jar;
create temporary function myfun as 'UDFMapStringInt2String'; 

set hiveconf hbase.zookeeper.quorum='hfa-pro0044.hadoop.cpcc.iflyyun.cn,hfa-pro0045.hadoop.cpcc.iflyyun.cn,hfa-pro0002.hadoop.cpcc.iflyyun.cn'

insert overwrite table hbase_ids
select
	global_id,
  myfun(imei),                                                         
  myfun(mac),                                                            
  myfun(imsi),                                                           
  myfun(phone_number),                                                   
  myfun(idfa),                                                           
  myfun(openudid),                                                       
  myfun(uid),                                                            
  myfun(android_id)
from
  ids
where product='release' and day='${hivevar:day}';    

insert overwrite table hbase_index
select
  id,
  global_id
from
  index
where day='${hivevar:day}';    
