#!/bin/sh

rm -f cmd.txt

DAY=$1
INPUT_PATH="hdfs://ns-hf/user/compass/public/hive/idmapping/ids/product=original/day=$DAY"
OUTPUT_PATH_PREFIX='hdfs://ns-hf/user/compass/public/hive/idmapping/data/idmapping_converge/spark'

for PRODUCT in imei mac idfa openudid phonenumber imsi; do
  for step in 1 2 3; do 
    echo "**********  start run ${PRODUCT} step ${step}  **********\n"
       
    hadoop fs -rm -r $OUTPUT_PATH_PREFIX"/product="${PRODUCT}"_step_"${step}

    echo "spark-submit --class idmapping.SparkIdMapping${step}  --master yarn-cluster --num-executors 100 --driver-memory 2g --executor-memory 15g --queue dmp ./zhangjie-1.0-SNAPSHOT-jar-with-dependencies.jar $INPUT_PATH   hdfs://ns-hf/user/compass/public/hive/idmapping/data/idmapping_converge/spark  ${PRODUCT}_step_${step} ${DAY} ${PRODUCT}" >> cmd.txt
    
    spark-submit --class idmapping.SparkIdMapping${step}  --master yarn-cluster --num-executors 100 --driver-memory 2g --executor-memory 15g --queue dmp ./zhangjie-1.0-SNAPSHOT-jar-with-dependencies.jar $INPUT_PATH   $OUTPUT_PATH_PREFIX  ${PRODUCT}_step_${step} ${DAY} ${PRODUCT}
    
    if [ $? -eq 0 ];then
      echo "\n**********  ${PRODUCT} step ${step} success  **********\n"
    else
      echo "\n**********  ${PRODUCT} step ${step} failed!  **********\n"
      exit 255
    fi
    
    INPUT_PATH=$OUTPUT_PATH_PREFIX"/product="${PRODUCT}"_step_"${step}"/day="${DAY}
  done
done
