package com.rz.utils

import com.ggstar.util.ip.IpHelper


/**
 * @Project sparksqlPro
 * @Name IpUtil
 * @Description IP解析工具类
 * @Auther Rz Lee
 * @Create 2020-02-15 3:23 PM
 * @Version 1.0
 */
object IpUtil {
  def getCity(ip: String)={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    val region = getCity("120.230.116.35")
    println(region)
  }

}
