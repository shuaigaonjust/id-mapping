#!/bin/sh

rm -f cmd.txt

DAY=$1
INPUT_PATH="hdfs://ns-hf/user/compass/public/hive/idmapping/ids/product=original/day=$DAY"
OUTPUT_PATH_PREFIX="hdfs://ns-hf/user/compass/public/hive/idmapping/ids_2"

for PRODUCT in imei mac idfa openudid phonenumber imsi; do
  for step in 1 2; do 
    echo "**********  start run ${PRODUCT} step ${step}  **********\n"
       
    hadoop fs -rm -r $OUTPUT_PATH_PREFIX"/product="${PRODUCT}"_step_"${step}"/day="${day}
  
    if [ ${PRODUCT} = imsi ] && [ ${step} = 2 ];then
           echo "spark-submit --class idmapping.SparkIdMapping${step}  --master yarn-cluster --conf spark.shuffle.manager=sort  --conf spark.network.timeout=480  --conf spark.shuffle.io.maxRetries=30 --conf spark.shuffle.memoryFraction=0.5  --executor-cores 3 --num-executors 100 --driver-memory 8g --executor-memory 16g --queue dmp ./IdMapping-1.0-SNAPSHOT-jar-with-dependencies.jar $INPUT_PATH   hdfs://ns-hf/user/compass/public/hive/idmapping/ids_2  release ${DAY} ${PRODUCT}" >> cmd.txt
           spark-submit --class idmapping.SparkIdMapping${step}  --master yarn-cluster --conf spark.shuffle.manager=sort  --conf spark.network.timeout=480  --conf spark.shuffle.io.maxRetries=30 --conf spark.shuffle.memoryFraction=0.5  --executor-cores 3 --num-executors 100 --driver-memory 8g --executor-memory 16g --queue dmp ./IdMapping-1.0-SNAPSHOT-jar-with-dependencies.jar $INPUT_PATH   $OUTPUT_PATH_PREFIX  release ${DAY} ${PRODUCT}
    else
           echo "spark-submit --class idmapping.SparkIdMapping${step}  --master yarn-cluster --conf spark.shuffle.manager=sort  --conf spark.network.timeout=480  --conf spark.shuffle.io.maxRetries=30 --conf spark.shuffle.memoryFraction=0.5  --executor-cores 3 --num-executors 100 --driver-memory 8g --executor-memory 16g --queue dmp ./IdMapping-1.0-SNAPSHOT-jar-with-dependencies.jar $INPUT_PATH   hdfs://ns-hf/user/compass/public/hive/idmapping/ids_2  ${PRODUCT}_step_${step} ${DAY} ${PRODUCT}" >> cmd.txt
           spark-submit --class idmapping.SparkIdMapping${step}  --master yarn-cluster --conf spark.shuffle.manager=sort  --conf spark.network.timeout=480  --conf spark.shuffle.io.maxRetries=30 --conf spark.shuffle.memoryFraction=0.5  --executor-cores 3 --num-executors 100 --driver-memory 8g --executor-memory 16g --queue dmp ./IdMapping-1.0-SNAPSHOT-jar-with-dependencies.jar $INPUT_PATH   $OUTPUT_PATH_PREFIX  ${PRODUCT}_step_${step} ${DAY} ${PRODUCT}
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
  for step in 1 2; do 
    if [ ${PRODUCT} = imsi ] && [ ${step} = 2 ];then
      echo "not remove release"
    else
      hadoop fs -rm -r $OUTPUT_PATH_PREFIX"/product="${PRODUCT}"_step_"${step}
    fi
  done
 done