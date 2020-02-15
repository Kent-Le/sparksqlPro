package com.rz.utils


import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
 * @Project sparksqlPro
 * @Name AccessCovertUtil
 * @Description
 * @Auther Rz Lee
 * @Create 2020-02-02 11:34 AM
 * @Version 1.0
 */
object AccessCovertUtil {
  val struct: StructType = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)

    )
  )


  def parseLog(log: String) ={
    try{
      val splits: Array[String] = log.split("\t")
      val url: String = splits(1)
      val traffic: Long = splits(2).toLong
      val ip = splits(3)
      val domain = "https://www.rz.com/"
      val cms: String = url.substring(url.indexOf(domain)+domain.length)
      val cmsTypeId: Array[String] = cms.split("/")
      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length>1){
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }
      val city =IpUtil.getCity(ip)
      val time = splits(0)
      val day: String = time.substring(0,10).replace("-", "")
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    }catch {
      case _: Exception =>{
        Row(0)
      }
    }

  }

  

}
