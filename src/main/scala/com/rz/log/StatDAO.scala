package com.rz.log

import java.sql.{Connection, PreparedStatement}

import com.rz.utils.MysqlUtil

import scala.collection.mutable.ListBuffer

/**
 * @Project sparksqlPro
 * @Name StatDao
 * @Description 各维度统计的DAO操作
 * @Auther Rz Lee
 * @Create 2020-02-15 4:49 PM
 * @Version 1.0
 */
object StatDAO {
  def insertDayVideoAccessStat(list: ListBuffer[DayVideoAccessStat])={
    var connection: Connection = null

    var pstmt: PreparedStatement = null
    try{
      connection = MysqlUtil.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into day_video_access_topn_stat(day,cmsId,times) values(?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for( el <- list){
        pstmt.setString(1, el.day)
        pstmt.setLong(2, el.cmsId)
        pstmt.setLong(3, el.times)
        pstmt.addBatch()
      }
      pstmt.executeBatch() // 执行指处理
      connection.commit() // 手工提交
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MysqlUtil.release(connection, pstmt)
    }
  }


  def insertDayVideoCityAccessStat(list: ListBuffer[DayVideoCityAccessStat])={
    var connection: Connection = null

    var pstmt: PreparedStatement = null
    try{
      connection = MysqlUtil.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into day_video_city_access_topn_stat(day,cmsId,city,times,times_rank) values(?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for( el <- list){
        pstmt.setString(1, el.day)
        pstmt.setLong(2, el.cmsId)
        pstmt.setString(3, el.city)
        pstmt.setLong(4, el.times)
        pstmt.setInt(5, el.times_rank)
        pstmt.addBatch()
      }
      pstmt.executeBatch() // 执行指处理
      connection.commit() // 手工提交
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MysqlUtil.release(connection, pstmt)
    }
  }

}
