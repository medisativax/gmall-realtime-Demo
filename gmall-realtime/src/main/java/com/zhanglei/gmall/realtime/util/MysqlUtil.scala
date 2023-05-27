package com.zhanglei.gmall.realtime.util

object MysqlUtil {
  def getBaseDicLookUpDDL: String ={
    return """
      |create TEMPORARY table base_dic(
      |    `dic_code` String,
      |    `dic_name` String,
      |    `parent_code` String,
      |    `create_time` timestamp,
      |    `operate_time` timestamp,
      |    primary key(`dic_code`) not enforced
      |)
      |""".stripMargin + mysqlLookUpTableDDL("base_dic")
  }

  def mysqlLookUpTableDDL(tableName: String): String ={
    return s"""
      | WITH (
      |  'connector' = 'jdbc',
      |  'url' = 'jdbc:mysql://hadoop01:3306/gmall',
      |  'table-name' = '$tableName',
      |  'lookup.cache.max-rows' = '10',
      |  'lookup.cache.ttl' = '1 hour',
      |  'driver' = 'com.mysql.cj.jdbc.Driver'
      |  'username' = 'root',
      |  'password' = 'root'
      |)
      |""".stripMargin
  }

}
