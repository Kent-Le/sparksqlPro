package com.rz.log

/**
 * @Project sparksqlPro
 * @Name DayVideoAccessStat
 * @Description 每次课程访问次数实体类
 * @Auther Rz Lee
 * @Create 2020-02-15 4:47 PM
 * @Version 1.0
 */
case class DayVideoCityAccessStat(day:String, cmsId: Long, city: String, times: Long, times_rank: Int)
