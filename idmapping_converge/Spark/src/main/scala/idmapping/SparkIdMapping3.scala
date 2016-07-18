package idmapping

import ids.IDs
import ids.IDsOutputFormat
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by zhangjie on 16/6/15.
  */
object SparkIdMapping3 {

  /*取出AvroKey封装的IDs对象，将id对象toString作为key参与groupByKey*/
  def data2IDs(tempAvro:(AvroKey[IDs],NullWritable)) : (String,IDs) = {
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

      (tid.toString,tid)
  }

  /*groupByKey后的group，只输出一个IDs，相当于distinct操作*/
  def combineValueThree(tuple: (String,Iterable[IDs])): (NullWritable,IDs) = {

    val id =  tuple._2.iterator.next()
    val tempGlobalId = CommonUtil.getOneGlobalId(id)
    //如果global_id为空,说明所有id都为空，不输出
    if (!(tempGlobalId.equals(""))){
       id.setGlobalId(tempGlobalId)
       (null,id)
    }else{
       null
    }
  }


  def main(args: Array[String]) {
    /*
    val spark_home = "D:/spark-1.6.0-bin-hadoop2.6"
    val conf = new SparkConf().setAppName("idmapping converge id").setSparkHome(spark_home).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val df = sc.newAPIHadoopFile[AvroKey[IDs], NullWritable, AvroKeyInputFormat[IDs]]("output2/")
    val out = df.map(data2IDs).groupByKey().map(combineValueThree)
    out.saveAsNewAPIHadoopFile[IDsOutputFormat]("output3")
*/

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

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
      System.err.println("id must be one of <imei> <mac> <idfa> <openudid> <phonenumber> <imsi>!")
      System.exit(-1)
    }

    val df = sc.newAPIHadoopFile[AvroKey[IDs], NullWritable, AvroKeyInputFormat[IDs]](input)
    val out = df.map(data2IDs).groupByKey().map(combineValueThree)
    out.saveAsNewAPIHadoopFile[IDsOutputFormat](output + "/" +"product=" + dataname + "/" + "day=" + datatime + "/")
  }
}
