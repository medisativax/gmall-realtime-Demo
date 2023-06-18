package com.zhanglei.gmall.realtime.app.func

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.alibaba.fastjson.JSONObject
import com.zhanglei.gmall.realtime.bean.TradeTrademarkCategoryUserSpuOrderBean
import com.zhanglei.gmall.realtime.util.{DimUtil, DruidDSUtil, ThreadPoolUtil}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import java.util.concurrent.ThreadPoolExecutor

abstract class DimAsyncAFunction[T] extends RichAsyncFunction[T, T] {
  var dataSource: DruidDataSource = _
  var threadPoolExecutor: ThreadPoolExecutor = _
  var tableName: String = _

  def this(tableName: String) {
    this()
    this.tableName = tableName
  }

  override def open(parameters: Configuration): Unit = {
    dataSource = DruidDSUtil.createDataSource()

    threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor
  }

  def getkey(input: T): String

  def join(input: T, dimInfo: JSONObject): Unit

  override def asyncInvoke(input: T, resultFuture: ResultFuture[T]): Unit = {
    threadPoolExecutor.execute(new Runnable {
      override def run(): Unit = {
        try {
          //获取连接
          val connection: DruidPooledConnection = dataSource.getConnection()
          //查询维表获取维度信息
          val key: String = getkey(input)
          val dimInfo: JSONObject = DimUtil.getDimInfo(connection, tableName, key)
          //降维度信息补充至当前数据
          if (dimInfo != null) {
            join(input, dimInfo)
          }
          //归还连接
          connection.close()
          //将数据写出
          resultFuture.complete(List(input))
        } catch {
          case e: Exception => println("关联维度表失败" + input + "Table:" + tableName);e.printStackTrace()
        }
      }
    })
  }

  override def timeout(input: T, resultFuture: ResultFuture[T]): Unit = {
    println("TimeOut" + input)
  }
}
