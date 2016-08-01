#!/bin/bash

rm -f cmd.txt

DAY=$1
INPUT_PATH="/user/compass/public/hive/idmapping/ids/product=original/day=$DAY"
OUTPUT_PATH_PREFIX="/user/compass/public/hive/idmapping/ids_2"

for PRODUCT in imei mac idfa openudid phonenumber imsi; do
  for step in 1 2 3; do 
    echo "**********  start run ${PRODUCT} step ${step}  **********\n"
    
    hadoop fs -rm -r $OUTPUT_PATH_PREFIX"/product="${PRODUCT}"_step_"${step}
   
    if [ ${PRODUCT} = imsi ] && [ ${step} = 3 ];then
           echo "hadoop jar IdMapping-0.1.0-SNAPSHOT-jar-with-dependencies.jar com.iflytek.hadoop.idmapping.main.IdMappingMain${step}  -Dmapred.reduce.tasks=110 -Dmapreduce.task.timeout=3600000  -Dmapreduce.map.memory.mb=9216 -Dmapreduce.reduce.memory.mb=9216 -Dmapreduce.map.java.opts=-Xmx9216m -Dmapreduce.reduce.java.opts=-Xmx9216m  -Dmapred.job.shuffle.merge.percent=0.6  -Dmapreduce.job.queuename=dmp  $INPUT_PATH  $OUTPUT_PATH_PREFIX  release ${DAY} ${PRODUCT}" >> cmd.txt
           hadoop jar IdMapping-0.1.0-SNAPSHOT-jar-with-dependencies.jar com.iflytek.hadoop.idmapping.main.IdMappingMain${step}  -Dmapred.reduce.tasks=110 -Dmapreduce.task.timeout=3600000  -Dmapreduce.map.memory.mb=9216 -Dmapreduce.reduce.memory.mb=9216 -Dmapreduce.map.java.opts=-Xmx9216m -Dmapreduce.reduce.java.opts=-Xmx9216m  -Dmapred.job.shuffle.merge.percent=0.6  -Dmapreduce.job.queuename=dmp  $INPUT_PATH  $OUTPUT_PATH_PREFIX release ${DAY} ${PRODUCT}
    else
           echo "hadoop jar IdMapping-0.1.0-SNAPSHOT-jar-with-dependencies.jar com.iflytek.hadoop.idmapping.main.IdMappingMain${step}  -Dmapred.reduce.tasks=110 -Dmapreduce.task.timeout=3600000  -Dmapreduce.map.memory.mb=9216 -Dmapreduce.reduce.memory.mb=9216 -Dmapreduce.map.java.opts=-Xmx9216m -Dmapreduce.reduce.java.opts=-Xmx9216m  -Dmapred.job.shuffle.merge.percent=0.6  -Dmapreduce.job.queuename=dmp  $INPUT_PATH  $OUTPUT_PATH_PREFIX  ${PRODUCT}_step_${step} ${DAY} ${PRODUCT}" >> cmd.txt
           hadoop jar IdMapping-0.1.0-SNAPSHOT-jar-with-dependencies.jar com.iflytek.hadoop.idmapping.main.IdMappingMain${step}  -Dmapred.reduce.tasks=110 -Dmapreduce.task.timeout=3600000  -Dmapreduce.map.memory.mb=9216 -Dmapreduce.reduce.memory.mb=9216 -Dmapreduce.map.java.opts=-Xmx9216m -Dmapreduce.reduce.java.opts=-Xmx9216m  -Dmapred.job.shuffle.merge.percent=0.6  -Dmapreduce.job.queuename=dmp  $INPUT_PATH  $OUTPUT_PATH_PREFIX ${PRODUCT}_step_${step} ${DAY} ${PRODUCT}
    fi

    if [ $? -eq 0 ];then
      echo "\n**********  ${PRODUCT} step ${step} success  **********\n"
    else
      echo "\n**********  ${PRODUCT} step ${step} failed!  **********\n"
      exit 255
    fi
    
    INPUT_PATH=$OUTPUT_PATH_PREFIX"/product="${PRODUCT}"_step_"${step}"/day="${DAY}
  done
done

for PRODUCT in imei mac idfa openudid phonenumber imsi; do
  for step in 1 2 3; do 
    if [ ${PRODUCT} = imsi ] && [ ${step} = 3 ];then
      echo "not remove release"
    else
      hadoop fs -rm -r $OUTPUT_PATH_PREFIX"/product="${PRODUCT}"_step_"${step}
    fi
  done
 done