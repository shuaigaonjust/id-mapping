package idmapping

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random
import ids.IDs

/**
  * create by zhangjie
  * 2016-06-14
  */
object CommonUtil{

  val commonStr = "abcdefghijklmnopqrstuvwxyz123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  var id = "imei"


  def getRandomString(str : String): String = {
      this.synchronized {
        val base = str
        val random = new Random()
        val strBuffer = new StringBuffer
        for (i <- 0 until base.length) {
          val num = random.nextInt(base.length)
          strBuffer.append(base.charAt(num))
        }
        strBuffer.toString
      }
  }

  def getOneGlobalId(ids: IDs): String = {
      this.synchronized {
          if (ids.getImei.size != 0) {
            ids.getImei.keySet.iterator.next
          } else if (ids.getMac.size != 0) {
            ids.getMac.keySet.iterator.next
          } else if (ids.getIdfa.size != 0) {
            ids.getIdfa.keySet.iterator.next
          } else if (ids.getOpenudid.size != 0) {
            ids.getOpenudid.keySet.iterator.next
          } else if (ids.getPhoneNumber.size != 0) {
            ids.getPhoneNumber.keySet.iterator.next
          } else if (ids.getImsi.size != 0) {
            ids.getImsi.keySet.iterator.next
          } else if (ids.getUid.size != 0) {
            ids.getUid.keySet.iterator.next
          } else if (ids.getDid.size != 0) {
            ids.getDid.keySet.iterator.next
          } else {
            ""
          }
      }
  }

  def initIdsJ(ids:IDs): Unit = {
    this.synchronized {
      ids.setGlobalId("")
      ids.setImei(new java.util.HashMap[String, Integer])
      ids.setMac(new java.util.HashMap[String, Integer])
      ids.setIdfa(new java.util.HashMap[String, Integer])
      ids.setOpenudid(new java.util.HashMap[String, Integer])
      ids.setPhoneNumber(new java.util.HashMap[String, Integer])
      ids.setImsi(new java.util.HashMap[String, Integer])
      ids.setUid(new java.util.HashMap[String, Integer])
      ids.setDid(new java.util.HashMap[String, Integer])
    }
  }

  def initIds(ids:IDs): Unit ={
    this.synchronized {
      ids.setGlobalId("")
      ids.setImei(new mutable.HashMap[String, Integer])
      ids.setMac(new mutable.HashMap[String, Integer])
      ids.setIdfa(new mutable.HashMap[String, Integer])
      ids.setOpenudid(new mutable.HashMap[String, Integer])
      ids.setPhoneNumber(new mutable.HashMap[String, Integer])
      ids.setImsi(new mutable.HashMap[String, Integer])
      ids.setUid(new mutable.HashMap[String, Integer])
      ids.setDid(new mutable.HashMap[String, Integer])
  }
  }

  def addToMap(idSourceMap:java.util.Map[String,Integer],idDesMap:java.util.Map[String,Integer]): Boolean ={
    this.synchronized {
      val tempIt = idSourceMap.entrySet().iterator()
      while (tempIt.hasNext) {
        val entry = tempIt.next()
        val k = entry.getKey
        val v = entry.getValue
        if (idDesMap.contains(k)) {
          if (v < idDesMap(k)) {
            idDesMap.put(k, v)
          }
        } else {
          idDesMap.put(k, v)
        }
      }
      if (idDesMap.size > 10) {
        true
      } else {false}
    }
  }

  def mergeIdAndJudgeSize(idSource:IDs, idDes:IDs): Boolean ={
    this.synchronized {
      return addToMap(idSource.getImei, idDes.getImei) ||
        addToMap(idSource.getMac, idDes.getMac) ||
        addToMap(idSource.getIdfa, idDes.getIdfa) ||
        addToMap(idSource.getOpenudid, idDes.getOpenudid) ||
        addToMap(idSource.getPhoneNumber, idDes.getPhoneNumber) ||
        addToMap(idSource.getImsi, idDes.getImsi) ||
        addToMap(idSource.getUid, idDes.getUid) ||
        addToMap(idSource.getDid, idDes.getDid)
    }
  }

  def mergeId(idSource:IDs, idDes:IDs){
    this.synchronized {
      addToMap(idSource.getImei, idDes.getImei)
      addToMap(idSource.getMac, idDes.getMac)
      addToMap(idSource.getIdfa, idDes.getIdfa)
      addToMap(idSource.getOpenudid, idDes.getOpenudid)
      addToMap(idSource.getPhoneNumber, idDes.getPhoneNumber)
      addToMap(idSource.getImsi, idDes.getImsi)
      addToMap(idSource.getUid, idDes.getUid)
      addToMap(idSource.getDid, idDes.getDid)
    }
  }

  def getMapByIdType(idType : String,tid : IDs): Map[String,Integer] ={
    this.synchronized {
      if (idType == "imei") {
        tid.getImei.toMap
      } else if (idType == "mac") {
        tid.getMac.toMap
      } else if (idType == "idfa") {
        tid.getIdfa.toMap
      } else if (idType == "openudid") {
        tid.getOpenudid.toMap
      } else if (idType == "phonenumber") {
        tid.getPhoneNumber.toMap
      } else if (idType == "imsi"){
        tid.getImsi.toMap
      } else {
        null
      }
    }
  }
}