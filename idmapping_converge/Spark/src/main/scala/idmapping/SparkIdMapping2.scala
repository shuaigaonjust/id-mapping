package idmapping

import ids.IDs
import ids.IDsOutputFormat
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

/**
  * create by zhangjie
  * 2016-06-14
  */
object SparkIdMapping2 {
  def idsToSecondKey(tempAvro:(AvroKey[IDs],NullWritable)) : (String, IDs) = {
    /*取出AvroKey封装的IDs对象*/
    val tid = new IDs()
    CommonUtil.initIdsJ(tid)
    tid.setGlobalId(tempAvro._1.datum().getGlobalId)
    tid.getImei.putAll(tempAvro._1.datum().getImei)
    tid.getMac.putAll(tempAvro._1.datum().getMac)
    tid.getIdfa.putAll(tempAvro._1.datum().getIdfa)
    tid.getOpenudid.putAll(tempAvro._1.datum().getOpenudid)
    tid.getImsi.putAll(tempAvro._1.datum().getImsi)
    tid.getPhoneNumber.putAll(tempAvro._1.datum().getPhoneNumber)
    tid.getUid.putAll(tempAvro._1.datum().getUid)
    tid.getDid.putAll(tempAvro._1.datum().getDid)
    //      tid.getAndroidId.putAll(tempAvro._1.datum().getAndroidId)

    //取出存在global_id中的secondkey
    var secondKey = tid.getGlobalId()
    //判断secondkey是否为空，为空则secondkey赋随机数
    if (secondKey.length == 0){
      secondKey =  CommonUtil.id + "_" + CommonUtil.getRandomString()
    }
    (secondKey,tid)
  }

  /*处理groupByKey聚合后的每一个group*/
  def combineValueTwo(tuple: (String,Iterable[IDs])): Array[(NullWritable,IDs)] = {

    //当前id的map为空，不参与聚合，直接写
    if (tuple._1.startsWith(CommonUtil.id + "_")){
      val idss = new ArrayBuffer[(NullWritable,IDs)]
      val tupleIt = tuple._2.iterator
      while (tupleIt.hasNext) {
        val id = tupleIt.next()
        val tempGlobalId = CommonUtil.getOneGlobalId(id)
        //如果global_id为空,说明所有id都为空，不输出
        if (!(tempGlobalId.equals(""))) {
          id.setGlobalId(tempGlobalId)
          idss.append((null, id))
        }
      }
      idss.toArray
    }else {
      val idss = new ArrayBuffer[(NullWritable,IDs)]
      val tupleArray = tuple._2.toArray

      val tempIds = new IDs
      //初始化tempIds
      CommonUtil.initIds(tempIds)
      for(id <- tupleArray) {
        //聚合当前group中所有IDs对象中的Map
        CommonUtil.mergeId(id,tempIds)
      }
      val tempGlobalId = CommonUtil.getOneGlobalId(tempIds)
      //如果global_id为空,说明所有id都为空，不输出
      if (!(tempGlobalId.equals(""))) {
        tempIds.setGlobalId(tempGlobalId)
        idss.append((null,tempIds))
      }
      idss.toArray
    }
  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.setLong(CommonUtil.SPLIT_MAXSIZE,64 * 1024 * 1024)
    sc.hadoopConfiguration.setLong(CommonUtil.SPLIT_MINSIZE,32 * 1024 * 1024)
    val job = new Job(sc.hadoopConfiguration)

    val length = args.length
    if (length < 5){
      System.err.println("args must be <inputPath> <outputPath> <dataname> <datattime> <id>")
      System.exit(-1)
    }

    val input = args(length-5)
    val output = args(length-4)
    val dataname = args(length-3)
    val datatime = args(length-2)
    CommonUtil.id = args(length-1)
    if (!CommonUtil.id.equals("imei") && !CommonUtil.id.equals("mac")
      && !CommonUtil.id.equals("idfa") && !CommonUtil.id.equals("openudid")
      && !CommonUtil.id.equals("phonenumber") && !CommonUtil.id.equals("imsi")){
      System.err.println("Id must be one of <imei> <mac> <idfa> <openudid> <phonenumber> <imsi>!")
      System.exit(-1)
    }

    val df = sc.newAPIHadoopFile[AvroKey[IDs], NullWritable, AvroKeyInputFormat[IDs]](input,classOf[AvroKeyInputFormat[IDs]],
      classOf[AvroKey[IDs]], classOf[NullWritable], job.getConfiguration)
    val out = df.map(idsToSecondKey).distinct().groupByKey().flatMap(combineValueTwo)
    out.saveAsNewAPIHadoopFile[IDsOutputFormat](output + "/" +"product=" + dataname + "/" + "day=" + datatime + "/")
  }
}
