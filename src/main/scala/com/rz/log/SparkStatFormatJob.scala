package com.rz.log

import com.rz.utils.DateUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @Project sparksqlPro
 * @Name SparkStatFormatJb
 * @Description
 * @Auther Rz Lee
 * @Create 2020-02-02 9:09 AM
 * @Version 1.0
 */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[2]").getOrCreate()
    val access: RDD[String] = spark.sparkContext.textFile("")
    access.map(line=>{
      val splited: Array[String] = line.split("  ")
      val ip: String = splited(0)
      /** 原始日志的第三个和第四个字段拼接起来就是完整的访问时间：
       * [10/Nov/2020:00:01:02 +0800] =====>yyyy-MM-dd HH:mm:ss
       * */
      val time: String = splited(3) + "  " + splited(4)
      val url: String = splited(11).replaceAll("\"", "")
      val traffic: String = splited(9)
      DateUtil.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    })
  }
}
