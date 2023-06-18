package com.zhanglei.gmall.realtime.app.func

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.alibaba.fastjson.JSONObject
import com.zhanglei.gmall.realtime.util.{DimUtil, DruidDSUtil, PhoenixUtil}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class DimSinkFunction extends RichSinkFunction[JSONObject]{
  var druidDataSource: DruidDataSource = _

  override def open(parameters: Configuration): Unit = {
    druidDataSource = DruidDSUtil.createDataSource()
  }
  // value : {"database":"gmall","table":"base_trademark","type":"update","ts":1684385691,"xid":16725,"commit":true,
  //  // "data":{"id":13,"tm_name":"root"},"old":{"logo_url":"/aaa/aaa"},"sinkTable":"dim_xxx"}
  override def invoke(value: JSONObject, context: SinkFunction.Context): Unit = {
    // 获得连接
    val connection: DruidPooledConnection = druidDataSource.getConnection()
    val sinkTable: String = value.getString("sinkTable")
    val data: JSONObject = value.getJSONObject("data")

    //获取数据类型
    val valuetype: String = value.getString("type")
    if (valuetype.equals("update")) {
      DimUtil.delDimInfo(sinkTable.toUpperCase(), data.getString("id"))
    }

    // 写出数据
    try {
      PhoenixUtil.upsertValues(connection, sinkTable, data)
    } catch {
      case exception: Exception => throw exception
    }

    // 归还连接
    connection.close()
  }
}
