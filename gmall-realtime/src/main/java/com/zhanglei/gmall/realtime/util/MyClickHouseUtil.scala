package com.zhanglei.gmall.realtime.util

import com.zhanglei.gmall.realtime.bean.TransientSink
import com.zhanglei.gmall.realtime.common.GmallConfig
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.lang.reflect.Field
import java.sql.PreparedStatement
import scala.util.control.Breaks.{break, breakable}


object MyClickHouseUtil {
  def getSinkFunction[T](sql: String): SinkFunction[T] = {

    return JdbcSink.sink(
      sql,
      new JdbcStatementBuilder[T] {
        override def accept(t: PreparedStatement, u: T): Unit = {
          //使用反射获取u对象中的数据
          val tclazz: Class[_ <: T] = u.getClass
          val fields: Array[Field] = tclazz.getDeclaredFields

          var offset = 0
          for (i <- fields.indices) {
            breakable{
              val field: Field = fields(i)
              field.setAccessible(true)
              // 尝试获取注解
              val transientSink: TransientSink = field.getAnnotation(classOf[TransientSink])
              if (transientSink != null) {
                offset += 1
                break()
              }
              // 获取属性
              val value: AnyRef = field.get(u)
              // 给 占位符(?) 赋值
              t.setObject(i + 1 - offset, value)
            }
          }
        }
      },
      new JdbcExecutionOptions.Builder()
        .withBatchSize(5)
        .withBatchIntervalMs(1000L)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
        .withUrl(GmallConfig.CLICKHOUSE_URL)
        .withPassword(GmallConfig.CLICKHOUSE_PASSWORD)
        .build())
  }
}
