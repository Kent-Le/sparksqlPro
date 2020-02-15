package com.rz.log

import com.rz.utils.AccessCovertUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @Project sparksqlPro
 * @Name SparkStatCleanJob
 * @Description
 * @Auther Rz Lee
 * @Create 2020-02-02 11:48 AM
 * @Version 1.0
 */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    if (args.length<2){
      println("Usage: TopNStatJob <inputPath> <outputPath")
      System.exit(1)
    }
    val Array(inputPath,outputPath) = args

    val spark = SparkSession.builder().appName(s"${this.getClass.getSimpleName}")
      .master("local[2]").getOrCreate()

    val accessRDD: RDD[String] = spark.sparkContext.textFile(inputPath)
    val df = spark.createDataFrame(accessRDD.map(line=>AccessCovertUtil.parseLog(line)),AccessCovertUtil.struct)
    // df.printSchema()
    // df.show(false)
    df.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save(outputPath)

    spark.stop()
  }
}
