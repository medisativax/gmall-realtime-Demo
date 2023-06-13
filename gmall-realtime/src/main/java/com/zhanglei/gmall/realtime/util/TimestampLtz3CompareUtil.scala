package com.zhanglei.gmall.realtime.util

object TimestampLtz3CompareUtil {
  // 数据格式 2022-04-01 10:20:47.302Z
  def compare(timestamp1: String, timestamp2: String): Int = {
    // 1. 去除末尾的时区标志，'Z' 表示 0 时区
    val cleanedTime1 = timestamp1.substring(0, timestamp1.length -1)
    val cleanedTime2 = timestamp2.substring(0, timestamp2.length -1)
    // 2.提取小于 1秒的部分
    val timeArr1: Array[String] = cleanedTime1.split("\\.")
    val timeArr2: Array[String] = cleanedTime2.split("\\.")
    val stringbuilder1: String = new StringBuilder("000").append(timeArr1(timeArr1.length - 1)).toString
    val stringbuilder2: String = new StringBuilder("000").append(timeArr2(timeArr2.length - 1)).toString
    val microseconds1: String = stringbuilder1.substring(stringbuilder1.length-3,stringbuilder1.length)
    val microseconds2: String = stringbuilder2.substring(stringbuilder2.length-3,stringbuilder2.length)
    val micro1: Int = microseconds1.toInt
    val micro2: Int = microseconds2.toInt
    // 3. 提取 yyyy-MM-dd HH:mm:ss 的部分
    val date1: String = timeArr1(0)
    val date2: String = timeArr2(0)
    val ts1: Long = DateFormatUtil.toTs(date1, isFull = true)
    val ts2: Long = DateFormatUtil.toTs(date2, isFull = true)
    // 4. 获得精确到毫秒的时间戳
    val microTs1: Long = ts1 + micro1
    val microTs2: Long = ts2 + micro2

    val divTs: Long = microTs1 - microTs2

    if (divTs < 0) -1
    else if (divTs == 0) 0
    else 1
  }

  def main(args: Array[String]): Unit = {
    println(compare("2023-06-12 11:59:37.408Z", "2023-06-12 11:59:37.408Z"))
  }
}
