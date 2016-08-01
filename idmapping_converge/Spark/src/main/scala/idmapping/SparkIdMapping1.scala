package idmapping

import ids.IDs
import ids.IDsOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapred.AvroKey

/**
  * Created by zhangjie on 16/6/14.
  */
object SparkIdMapping1 {
  def idsToDouble(tempAvro:(AvroKey[IDs],NullWritable)) : Array[(String, IDs)] = {
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

    //将所有IDs对象的global_id都置空
    tid.setGlobalId("")
    val idMap : Map[String,Integer] = CommonUtil.getMapByIdType(CommonUtil.id, tid)
    //当前id的map为空，不参与聚合，key赋为随机数
    if (idMap.isEmpty == true) {
      val aryBuffer = new ArrayBuffer[(String, IDs)]()
      aryBuffer.append((CommonUtil.id + "_" + CommonUtil.getRandomString(), tid))
      aryBuffer.toArray
    } else {
      val aryBuffer = new ArrayBuffer[(String, IDs)]()
      //选当前id的map中任意一个作为secondkey存入global_id中
      tid.setGlobalId(idMap.keySet.head)
      //当前id的map的keyset的每一个都作为key，散开
      for (k <- idMap.keySet) {
        aryBuffer.append((k, tid))
      }
      aryBuffer.toArray
    }
  }

  /*处理groupByKey聚合后的每一个group*/
  def combineValue(tuple: (String,Iterable[IDs])): Array[(NullWritable,IDs)] = {
    //当前id的map为空，不参与聚合，直接写
    if (tuple._1.startsWith(CommonUtil.id + "_")) {
      val idss = new ArrayBuffer[(NullWritable, IDs)]
      val tupleIt = tuple._2.iterator
      while (tupleIt.hasNext) {
        idss.append((null, tupleIt.next()))
      }
      idss.toArray
    } else {
      val idss = new ArrayBuffer[(NullWritable, IDs)]
      val secondkeys = new ArrayBuffer[String]()
      val tupleArray = tuple._2.toArray
      var tag = false
      val tempIds = new IDs
      CommonUtil.initIds(tempIds)
      breakable {
        for (id <- tupleArray) {
          val tempGlobalId = id.getGlobalId
          //取出distinct global_id存入array中
          if (secondkeys.contains(tempGlobalId) == false) {
            secondkeys.append(id.getGlobalId)
          }
          //本次循环的IDs对象id与tempIds聚合，并判断tempIds的各个id的map的size是否大于10个
          if (CommonUtil.mergeIdAndJudgeSize(id, tempIds) == true) {
            //tempIds的某个id的map的size大于10个，跳出循环
            tag = true
            break
          }
        }
      }
      //如果tag为true，当前group不聚合，全部数据直接写
      if (tag == true) {
        for (id1 <- tupleArray) {
          idss.append((null, id1))
        }
        idss.toArray
      } else {
        //设置全部聚合后tempIds的global_id为secondkey，循环写，写secondKeys.size条
        for (secondKey <- secondkeys) {
          tempIds.setGlobalId(secondKey)
          idss.append((null, tempIds))
        }
        idss.toArray
      }
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
      val out = df.flatMap(idsToDouble).groupByKey().flatMap(combineValue)
      out.saveAsNewAPIHadoopFile[IDsOutputFormat](output + "/" +"product=" + dataname + "/" + "day=" + datatime + "/")
  }
}
