package com.rz.utils




import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
 * @Project sparksqlPro
 * @Name DateUtil
 * @Description 日期时间解析工具类
 * @Auther Rz Lee
 * @Create 2020-02-02 9:18 AM
 * @Version 1.0
 */
object DateUtil {
  // 输入文件日期时间格式
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  // 目标日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(time: String)={
    TARGET_FORMAT.format(new Date(getTime(time))
    )
  }

  def getTime(time:String )={
    try{
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[")+1,time.lastIndexOf("]"))).getTime
    }catch {
      case _: Exception =>{
        0L
      }
    }
  }



}
