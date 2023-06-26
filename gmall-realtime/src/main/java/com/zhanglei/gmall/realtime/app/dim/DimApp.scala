package com.zhanglei.gmall.realtime.app.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import com.zhanglei.gmall.realtime.util.{MyKakfaUtil, PhoenixUtil}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.zhanglei.gmall.realtime.app.func.{DimSinkFunction, TableProcessFunction}
import com.zhanglei.gmall.realtime.bean.TableProcess
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.BroadcastStream

// Mock --> Mysql(logbin) --> Maxwell --> kafka(zookpeer) --> Dimapp --> Phoenix(hbase/zookeeper/HDFS)

object DimApp {
  def main(args: Array[String]): Unit = {
    //TODO 1.获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)   // 生产环境中设置为：kafka topic的分区数
////     1.1 开启Checkpoint (生产环境一定要开启)
//    env.enableCheckpointing(5 * 60000L,CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setCheckpointTimeout(10 * 60000L)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)    // 设置checkpoint的同时存在的数量
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L))    // 失败：每隔五秒重启一次，总共三次
////     1.2 设置状态后端 (生产环境一定要开启)
//    env.setStateBackend(new HashMapStateBackend())
//    env.getCheckpointConfig.setCheckpointStorage("hdfs://hadoop01:8020/gmall/ck")
//    System.setProperty("HADOOP_USER_NAME","root")

    //TODO 2.读取kakfa topic_db主题数据创建主流
    val topic = "topic_db"
    val groupId = "dim_app"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3.过滤掉非JSON数据&保留新增、变化以及初始化数据
    val filterJosnDS: DataStream[JSONObject] = kafkaDS.flatMap(new FlatMapFunction[String, JSONObject] {
      override def flatMap(t: String, collector: Collector[JSONObject]): Unit = {
        try {
          val JSONObject: JSONObject = JSON.parseObject(t)
          // 获取数据中的类型字段
          val type_field: String = JSONObject.getString("type")
          if (type_field.equals("insert") || type_field.equals("update") || type_field.equals("bootstrap-insert")) {
            collector.collect(JSONObject)
          }
        } catch {
          case e: Exception => println("发现脏数据" + t) // 可以实现侧边流
        }
      }
    })

    //TODO 4.使用FlinkCDC读取Mysql配置信息配置流
    val mySqlSource: MySqlSource[String] = MySqlSource.builder[String]()
      .hostname("hadoop01")
      .port(3306)
      .username("root")
      .password("root")
      .databaseList("gmall_config")
      .tableList("gmall_config.table_process")
      .startupOptions(StartupOptions.initial())
      .deserializer(new JsonDebeziumDeserializationSchema())
      .build()

    val mysqlSourceDS: DataStream[String] = env.fromSource(mySqlSource,
      WatermarkStrategy.noWatermarks(),
      "MysqlSource")

    //TODO 5.将配置流转换成为广播流
    val mapStateDescriptor: MapStateDescriptor[String, TableProcess] =
      new MapStateDescriptor[String, TableProcess]("map-state", classOf[String], classOf[TableProcess])

    val broadcastStream: BroadcastStream[String] = mysqlSourceDS.broadcast(mapStateDescriptor)

    //TODO 6.将主流与广播流连接
    val connectedStream: BroadcastConnectedStream[JSONObject, String] = filterJosnDS.connect(broadcastStream)

    //TODO 7.处理链接流，根据配置信息处理主流数据
    val dimDS: DataStream[JSONObject] = connectedStream.process(new TableProcessFunction(mapStateDescriptor))

    //TODO 8.将数据写入到Phoenix
    dimDS.print(">>>>>>>>>>>>>>>>>>")
    dimDS.addSink(new DimSinkFunction)

    //TODO 9.启动任务
    env.execute("DimApp")

  }
}
