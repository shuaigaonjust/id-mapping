#!/bin/bash
DAY=$1
current_day=$1
echo "****************** Start IDMapping Converge ${DAY} ******************" >> idmapping_$current_day.log
echo "****************** Start IDMapping Converge ${DAY} ******************" 

INPUT_PATH="/user/compass/public/hive/idmapping/ids_2/product=original/day=$DAY"
OUTPUT_PATH_PREFIX='/user/compass/public/hive/idmapping/ids_2/tmp/'

#for PRODUCT in imei mac idfa openudid phonenumber imsi; do
for PRODUCT in all; do
  for step in 1 2 3; do 
    hadoop fs -rm -r $OUTPUT_PATH_PREFIX"/product="${PRODUCT}"_step_"${step}"/day="${DAY}
    echo "**********  start run Converge ${DAY} step ${step}  **********\n" >> idmapping_$current_day.log
    echo "**********  start run Converge ${DAY} step ${step}  **********\n" 
    echo "hadoop jar idmapping_converge.jar com.iflytek.hadoop.idmapping.main.IdMappingMain${step}  -Dmapred.reduce.tasks=150 -Dmapreduce.map.memory.mb=9216 -Dmapreduce.reduce.memory.mb=9216 -Dmapreduce.map.java.opts=-Xmx9216m -Dmapreduce.reduce.java.opts=-Xmx9216m  -Dmapred.job.shuffle.merge.percent=0.5 -Dmapred.max.split.size=67108864  -Dmapreduce.job.queuename=dmp  $INPUT_PATH   $OUTPUT_PATH_PREFIX ${PRODUCT}_step_${step} ${DAY}" >> idmapping_$current_day.log
    echo "hadoop jar idmapping_converge.jar com.iflytek.hadoop.idmapping.main.IdMappingMain${step}  -Dmapred.reduce.tasks=150 -Dmapreduce.map.memory.mb=9216 -Dmapreduce.reduce.memory.mb=9216 -Dmapreduce.map.java.opts=-Xmx9216m -Dmapreduce.reduce.java.opts=-Xmx9216m  -Dmapred.job.shuffle.merge.percent=0.5 -Dmapred.max.split.size=67108864  -Dmapreduce.job.queuename=dmp  $INPUT_PATH   $OUTPUT_PATH_PREFIX ${PRODUCT}_step_${step} ${DAY}" 
    #hadoop jar idmapping_converge.jar com.iflytek.hadoop.idmapping.main.IdMappingMain${step}  -Dmapred.reduce.tasks=150 -Dmapreduce.map.memory.mb=9216 -Dmapreduce.reduce.memory.mb=9216 -Dmapreduce.map.java.opts=-Xmx9216m -Dmapreduce.reduce.java.opts=-Xmx9216m  -Dmapred.job.shuffle.merge.percent=0.5 -Dmapred.max.split.size=67108864  -Dmapreduce.job.queuename=dmp  $INPUT_PATH $OUTPUT_PATH_PREFIX ${PRODUCT}_step_${step} ${DAY}
    hadoop jar idmapping_converge.jar com.iflytek.hadoop.idmapping.main.IdMappingMain${step}  -Dmapred.reduce.tasks=150 -Dmapreduce.map.memory.mb=9216 -Dmapreduce.reduce.memory.mb=9216 -Dmapreduce.map.java.opts=-Xmx9216m -Dmapreduce.reduce.java.opts=-Xmx9216m  -Dmapred.job.shuffle.merge.percent=0.9 -Dmapreduce.job.queuename=dmp  $INPUT_PATH $OUTPUT_PATH_PREFIX ${PRODUCT}_step_${step} ${DAY}
    
    
    if [ $? -eq 0 ];then
      echo "\n********** Converge ${DAY} step ${step} success  **********\n" >> idmapping_$current_day.log
      echo "\n********** Converge ${DAY} step ${step} success  **********\n" 
    else
      echo "\n********** Converge ${DAY} step ${step} failed!  **********\n" >> idmapping_$current_day.log
      echo "\n********** Converge ${DAY} step ${step} failed!  **********\n"
      exit 255
    fi
    
    INPUT_PATH=$OUTPUT_PATH_PREFIX"/product="${PRODUCT}"_step_"${step}"/day="${DAY}
  done
done

hadoop fs -rm -r /user/compass/public/hive/idmapping/ids_2/product=release/day=${DAY}
hadoop fs -mv $OUTPUT_PATH_PREFIX/product=${PRODUCT}_step_3/day=${DAY} /user/compass/public/hive/idmapping/ids_2/product=release/
echo "Converge ${DAY} Task is done. mv data to /user/compass/public/hive/idmapping/ids_2/product=release/" >> idmapping_$current_day.log
echo "Converge ${DAY} Task is done. mv data to /user/compass/public/hive/idmapping/ids_2/product=release/"
hive -e "alter table idmapping.ids_2 drop partition (product='release',day='${DAY}')"
hive -e "alter table idmapping.ids_2 add partition (product='release',day='${DAY}') location '/user/compass/public/hive/idmapping/ids_2/product=release/day=${DAY}'"

#gen index
hadoop fs -rm -r hdfs://ns-hf/user/compass/public/hive/idmapping/index/day=${DAY}
hadoop jar idmapping_converge.jar com.iflytek.hadoop.idmapping.main.GenIndexUnique -Dmapred.reduce.tasks=200 -Dmapred.job.shuffle.merge.percent=0.7 -Dmapreduce.job.queuename=dmp /user/compass/public/hive/idmapping/ids_2/product=release/day=${DAY} /user/compass/public/hive/idmapping/tmp/index/day=${DAY}
hadoop fs -mv /user/compass/public/hive/idmapping/tmp/index/day=${DAY} /user/compass/public/hive/idmapping/index/
hive -e "alter table idmapping.index drop partition (day='${DAY}')"
hive -e "alter table idmapping.index add partition (day='${DAY}') location '/user/compass/public/hive/idmapping/index/day=${DAY}'"

