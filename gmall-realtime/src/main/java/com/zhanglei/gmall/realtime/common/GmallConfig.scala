package com.zhanglei.gmall.realtime.common

object GmallConfig {
  // Phoenix库名// Phoenix库名
  val HBASE_SCHEMA = "GMALL2022_REALTIME"

  // Phoenix驱动
  val PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver"

  // Phoenix连接参数
  val PHOENIX_SERVER = "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181"

  // ClickHouse驱动
  val CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver"

  // ClickHouse连接URL
  val CLICKHOUSE_URL = "jdbc:clickhouse://hadoop01:8123/gmall_rebuild"

  // ClickHouse连接Username
  val CLICKHOUSE_USERNAME = "root"

  // ClickHouse连接Password
  val CLICKHOUSE_PASSWORD = "root"
}
