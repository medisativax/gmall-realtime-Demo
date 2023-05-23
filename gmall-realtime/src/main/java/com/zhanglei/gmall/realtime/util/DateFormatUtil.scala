package com.zhanglei.gmall.realtime.util

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Date


object DateFormatUtil {
  private val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val dtfFull: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def toTs(dtStr: String, isFull: Boolean): Long ={
    var dtStrTemp = dtStr
    if (!isFull){
      dtStrTemp = dtStr + " 00:00:00"
    }
    val localDateTime: LocalDateTime = LocalDateTime.parse(dtStrTemp, dtfFull)
    return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli;
  }

  def toTs(dtStr: String): Long ={
    return toTs(dtStr,false)
  }

  def toDate(ts: Long): String ={
    val date = new Date(ts)
    val localDateTime: LocalDateTime = LocalDateTime.ofInstant(date.toInstant, ZoneId.systemDefault())
    return dtf.format(localDateTime)
  }

  def toYmdHms(ts: Long): String ={
    val date = new Date(ts)
    val localDateTime: LocalDateTime = LocalDateTime.ofInstant(date.toInstant, ZoneId.systemDefault())
    return dtfFull.format(localDateTime)
  }

}
