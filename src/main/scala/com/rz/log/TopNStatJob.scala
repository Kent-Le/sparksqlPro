package com.rz.log

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
/**
 * @Project sparksqlPro
 * @Name TopNStatJob
 * @Description
 * @Auther Rz Lee
 * @Create 2020-02-15 4:08 PM
 * @Version 1.0
 */
object TopNStatJob {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    if (args.length<1){
      println("Usage: TopNStatJob <inputPath> ")
      System.exit(1)
    }

    val Array(inputPath,outputPath) = args

    val spark = SparkSession.builder().appName(s"${this.getClass.getSimpleName}")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .master("local[2]").getOrCreate()

    val accessDF: DataFrame = spark.read.parquet(inputPath)

    // 最受欢迎的topN课程
    // videoAccessTopNStat(spark, accessDF)

    // 按照地市进行统计TopN课程
    cityAccessTopNStat(spark, accessDF)
    spark.stop()
  }

  /**
   * 最受欢迎的TopN课程
   * @param spark
   * @param accessDF
   */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame) = {
   /* import spark.implicits._
    val videoAccessToopNDF = accessDF.filter($"day" === "20170511" && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
    videoAccessToopNDF.show(false)*/

    accessDF.createOrReplaceTempView("access_logs")

    val videoAccessTopNDF = spark.sql("select day, cmsId, count(1) as times from access_logs " +
      "where day='20170511' and cmsType='video' " +
      "group by day, cmsId order by times desc")
    //videoAccessToopNDF.show(false)

    try{
      videoAccessTopNDF.foreachPartition(partitionOrRecords =>{
        val list = new ListBuffer[DayVideoAccessStat]
        partitionOrRecords.foreach(info =>{
          val day: String = info.getAs[String]("day")
          val cmsId: Long = info.getAs[Long]("cmsId")
          val times: Long = info.getAs[Long]("times")
          list.append(DayVideoAccessStat(day, cmsId, times))
        })
        StatDAO.insertDayVideoAccessStat(list)
      })
    }catch {
      case e:Exception =>e.printStackTrace()
    }
    
  }


  /**
   * 按照地市进行统计TopN课程
   * @param spark
   * @param accessDF
   */
  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame) = {
    import spark.implicits._
    val cityAccessTopNDF: DataFrame = accessDF.filter($"day" === "20170511" && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId").agg(count("cmsId").as("times"))
    // cityAccessTopNDF.show(false)

    // 窗口函数在spark sql中的使用
    val top3DF = cityAccessTopNDF.select(cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city")).orderBy(cityAccessTopNDF("times").desc)).as("times_rank")
    ).filter("times_rank <= 3") // Top 3
    top3DF.show(false)
    try{
      top3DF.foreachPartition(partitionOrRecords =>{
        val list = new ListBuffer[DayVideoCityAccessStat]
        partitionOrRecords.foreach(info =>{
          val day: String = info.getAs[String]("day")
          val cmsId: Long = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times: Long = info.getAs[Long]("times")
          val times_rank: Int = info.getAs[Int]("times_rank")
          list.append(DayVideoCityAccessStat(day, cmsId, city, times, times_rank))
        })
        StatDAO.insertDayVideoCityAccessStat(list)
      })
    }catch {
      case e:Exception =>e.printStackTrace()
    }

  }

}
