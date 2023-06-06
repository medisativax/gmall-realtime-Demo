package com.zhanglei.gmall.realtime.util

import com.alibaba.druid.pool.DruidPooledConnection
import com.alibaba.fastjson.JSONObject
import com.zhanglei.gmall.realtime.common.GmallConfig

import java.sql.PreparedStatement
import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object PhoenixUtil {
  /**
   *
   * @param connection  Phoenix连接
   * @param sinkTable   输出的表名
   * @param data        数据
   */
  def upsertValues(connection: DruidPooledConnection, sinkTable: String, data: JSONObject) = {
    // 1、拼接SQL语句：upsert into db.tn(id,name,sex) values ("1001","zhangsan","male")
    val columns: util.Set[String] = data.keySet()
    val values: util.Collection[AnyRef] = data.values()
    var sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" + columns.mkString(",") + ") values ('" + values.mkString("','") + "')"

    // 2、预编译SQL
    val statement: PreparedStatement = connection.prepareStatement(sql)

    // 3、执行SQL
    statement.execute()
    connection.commit()

    // 4、释放资源
    statement.close()
  }


}
