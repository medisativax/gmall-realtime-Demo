package com.zhanglei.gmall.realtime.app.func

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import com.zhanglei.gmall.realtime.bean.{GmallConfig, TableProcess}
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util
import scala.collection.mutable

class TableProcessFunction extends BroadcastProcessFunction[JSONObject,String,JSONObject]{

  var connection:Connection = _

  var mapStateDescriptor: MapStateDescriptor[String, TableProcess] = _

  def this(mapStateDescriptor:MapStateDescriptor[String, TableProcess]){
    this()
    this.mapStateDescriptor = mapStateDescriptor
  }

  override def open(parameters: Configuration): Unit = {
    connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER)
  }

  //  {"before":null,"after":{"source_table":"aa","sink_table":"aa","sink_columns":"va","sink_pk":"fa","sink_extend":"fas"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source",
//  "ts_ms":1684396334032,"snapshot":"false","db":"gmall-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
//  "op":"r","ts_ms":1684396334037,"transaction":null}
  override def processBroadcastElement(in2: String, context: BroadcastProcessFunction[JSONObject, String, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
    // 1.获取并解析数据
    val jSONObject: JSONObject = JSON.parseObject(in2)
    val tableProcess: TableProcess = JSON.parseObject(jSONObject.getString("after"), classOf[TableProcess])

    // 2.校验并建表
    checkTable(tableProcess.SinkTable,
      tableProcess.sinkColumns,
      tableProcess.sinkPk,
      tableProcess.sinkExtend)

    // 3.写入状态，广播出去
    val broadcastState: BroadcastState[String, TableProcess] = context.getBroadcastState(this.mapStateDescriptor)
    broadcastState.put(tableProcess.sourceTable,tableProcess)
  }

  /**
   * 校验并建表：create table if not exists db.tn(id varchar primary key,bb varchar,cc varchar) xxx
   * @param sinkTable     Phoenix表名
   * @param SinkColumns   phoenix表字段
   * @param SinkPk        phoenix表主键
   * @param SinkExtend    phoenix表扩展字段
   */
  def checkTable(sinkTable:String,SinkColumns:String,SinkPk:String,SinkExtend:String): Unit = {
    var perparStatement: PreparedStatement = null
    var sinkPk_filter = SinkPk
    var sinkExtend_filter = SinkExtend
    // 处理特殊字段
    try {
      if (SinkPk == null || "".equals(SinkPk)) {
        sinkPk_filter = "id"
      }
      if (SinkExtend == null) {
        sinkExtend_filter = ""
      }
      // 拼接SQl  create table if not exists db.tn(id varchar primary key,bb varchar,cc varchar) xxx
      val createTableSql: mutable.StringBuilder = new mutable.StringBuilder("create table if not exists ")
        .append(GmallConfig.HBASE_SCHEMA)
        .append(".")
        .append(sinkTable)
        .append("(")

      val columns: Array[String] = SinkColumns.split(",")
      columns.foreach(column => {
        // 判断是否为主键
        if (sinkPk_filter.equals(column)) {
          createTableSql.append(column).append(" varchar primary key")
        } else {
          createTableSql.append(column).append(" varchar")
        }
        // 判读是否为最后一个字段
        if (columns.indexOf(column) < columns.length - 1) { // 字段是唯一的
          createTableSql.append(",")
        }
      })

      createTableSql.append(")").append(sinkExtend_filter)

      // 编译SQL
      println("建表语句为：" + createTableSql)
      perparStatement = connection.prepareStatement(createTableSql.toString())

      // 执行SQL
      perparStatement.execute();
    } catch {
      case exception: Exception => throw new RuntimeException("建表失败" + sinkTable)
    } finally {
      // 释放资源
      if (perparStatement != null){
        perparStatement.close()
      }
    };
  }


  // in1 : {"database":"gmall","table":"base_trademark","type":"update","ts":1684385691,"xid":16725,"commit":true,
  // "data":{"id":13,"tm_name":"root","logo_url":"/aaa/ccc"},"old":{"logo_url":"/aaa/aaa"}}
  override def processElement(in1: JSONObject, readOnlyContext: BroadcastProcessFunction[JSONObject, String, JSONObject]#ReadOnlyContext, collector: Collector[JSONObject]): Unit = {
    // 1.获取广播流的配置数据
    val broadcastState: ReadOnlyBroadcastState[String, TableProcess] = readOnlyContext.getBroadcastState(this.mapStateDescriptor)
    val tableName: String = in1.getString("table")
    val tableProcess: TableProcess = broadcastState.get(tableName)

    if (tableProcess != null){
      // 2.过滤字段
      filterColumn(in1.getJSONObject("data"), tableProcess.sinkColumns)
      // 3.补充SinkTable并写入到流中
      in1.put("sinkTable",tableProcess.SinkTable)
      collector.collect(in1)
    }else{
      println("找不到对应key"+ tableName)
    }
  }

  /**
   * 过滤字段
   * @param data              {"id":13,"tm_name":"root","logo_url":"/aaa/ccc"}
   * @param sinkColumns       id,tm_name
   */
  def filterColumn(data: JSONObject, sinkColumns: String): Unit = {
    // 分割 sinkColumns
    val columns: Array[String] = sinkColumns.split(",")
    val entries: util.Set[util.Map.Entry[String, AnyRef]] = data.entrySet()

    //    import scala.collection.JavaConversions._
    //    for (entry <- entries){
    //      if (!columnsList.contains(entry.getKey)){
    //        entries.remove(entry.getKey)
    //      }
    //    }
    //  }
    entries.removeIf(next => !columns.contains(next.getKey))
  }

  override def close(): Unit = {
    connection.close()
  }
}
