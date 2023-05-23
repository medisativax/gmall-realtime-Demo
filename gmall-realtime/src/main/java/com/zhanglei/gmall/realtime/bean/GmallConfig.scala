package com.zhanglei.gmall.realtime.bean

object GmallConfig {
  // Phoenix库名// Phoenix库名
  val HBASE_SCHEMA = "GMALL2022_REALTIME"

  // Phoenix驱动
  val PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver"

  // Phoenix连接参数
  val PHOENIX_SERVER = "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181"

}
