for day in `seq -w {16,31}` 
do hive --hivevar year=2016 --hivevar month=05 --hivevar day=$day -f gen_ad_log.sql 
done;

for day in {20160415 20160501 20160508 20160515 20160522 20160602 20160609 20160616 20160623 20160630} 
do hive --hivevar day=$day -f "genlingxi.q" 
done;

for day in {02,09,16.23.30} 
do hive --hivevar day=$day -f lingxi201606.q 
done;

hive --hivevar year=2016 month=06 day=1 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=2 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=3 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=4 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=5 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=6 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=7 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=8 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=9 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=10 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=11 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=12 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=13 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=14 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=15 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=16 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=17 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=18 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=19 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=20 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=21 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=22 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=23 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=24 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=25 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=26 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=27 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=28 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=29 -f idmapping_gen_ids.q
hive --hivevar year=2016 month=06 day=30 -f idmapping_gen_ids.q