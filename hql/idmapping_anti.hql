USE idmapping;

set mapreduce.job.queuename=dmp;
set mapreduce.job.name=idmapping_antispam;
set hive.exec.compress.output=false;

dfs -rmr /user/compass/public/hive/idmapping/antispam/imei;
dfs -rmr /user/compass/public/hive/idmapping/antispam/mac;

FROM
(
  select distinct map_keys(imei)[0] as imei, map_keys(mac)[0] as mac
  from
    ids 
  where
    size(mac) > 0 and size(imei) > 0
    and product in ("ad_log", "sdk_log", "vcoam_log")
) a
INSERT OVERWRITE table anti_dict_imei
select imei
group by imei
having count(*) > 4
INSERT OVERWRITE table anti_dict_mac
select mac
group by mac
having count(*) > 4;


