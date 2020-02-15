package com.rz.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @Project sparksqlPro
 * @Name MysqlUtil
 * @Description
 * @Auther Rz Lee
 * @Create 2020-02-15 4:33 PM
 * @Version 1.0
 */
object MysqlUtil {

  /**
   * 获取数据库连接
   * @return
   */
  def getConnection()={
    DriverManager.getConnection("jdbc:mysql://10.211.55.12:3306/log_pro?user=root&password=root&useUnicode=true&characterEncoding=utf-8&useSSL=false")
  }

  /**
   * 释放数据库连接等资源
   * @param connection
   * @param pstmt
   */
  def release(connection:Connection, pstmt: PreparedStatement)={

    try{
      if (pstmt !=null){
        pstmt.close()
      }
    }catch {
      case e:Exception=> e.printStackTrace()
    }finally {
      if (connection != null){
        connection.close()
      }
    }

  }
}
