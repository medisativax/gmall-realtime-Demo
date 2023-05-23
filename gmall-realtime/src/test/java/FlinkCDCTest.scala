import com.alibaba.fastjson.{JSON, JSONObject}
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala._

import java.util
import java.util.Map

object FlinkCDCTest {
  def main(args: Array[String]): Unit = {
    val str:String =
      """
        |{"id":13,"tm_name":"root","logo_url":"/aaa/ccc"}
        |""".stripMargin
    filterColumn(JSON.parseObject(str),"id，tm_name")
  }


  def filterColumn(data: JSONObject, sinkColumns: String): Unit = {
    // 分割 sinkColumns
    val columns: Array[String] = sinkColumns.split("，")
    columns.foreach(println)

    val entries: util.Iterator[Map.Entry[String, AnyRef]] = data.entrySet().iterator()
    while (entries.hasNext){
      val entry: Map.Entry[String, AnyRef] = entries.next()
      if (!columns.contains(entry.getKey)){
        println("===="+entry.getKey)
        entries.remove()
      }
    }

    while (entries.hasNext){
      println(entries.next())
    }

  }
}
