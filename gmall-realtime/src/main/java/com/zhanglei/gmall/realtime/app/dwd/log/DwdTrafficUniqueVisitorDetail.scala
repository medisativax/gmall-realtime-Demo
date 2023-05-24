package com.zhanglei.gmall.realtime.app.dwd.log

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyKakfaUtil}
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFilterFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import java.lang

// 流程：web/app -> Nginx -> 日志服务器 -> Flume -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> flinkapp -> kafka(dwd)
// 程序流程：  mock(lg.sh) -> flume(f1.sh) -> kafka(zookeeper) -> BaseLogApp -> kafka(zookeeper) -> DwdTrafficUniqueVisitorDetail -> kafka(zookeeper)
object DwdTrafficUniqueVisitorDetail {
  def main(args: Array[String]): Unit = {
    //TODO 1.搭建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 生产环境中设置为：kafka topic的分区数
//    // 1.1 开启Checkpoint (生产环境一定要开启)
//    env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setCheckpointTimeout(10 * 60000L)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) // 设置checkpoint的同时存在的数量
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L)) // 失败：每隔五秒重启一次，总共三次
//    // 1.2 设置状态后端 (生产环境一定要开启)
//    env.setStateBackend(new HashMapStateBackend())
//    env.getCheckpointConfig.setCheckpointStorage("hdfs://hadoop01:8020/gmall/ck")
//    System.setProperty("HADOOP_USER_NAME", "root")

    //TODO 2.读取kafka 页面日志主题数据创建主键
    val topic = "dwd_traffic_page_log"
    val groupId = "unique_visitordetail"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3.过滤掉last_page_id不为Null，并将其转换成JSON对象
    val jsonObjectDS: DataStream[JSONObject] = kafkaDS.flatMap(new FlatMapFunction[String, JSONObject] {
      override def flatMap(t: String, collector: Collector[JSONObject]): Unit = {
        try {
          val jsonObject: JSONObject = JSON.parseObject(t)
          val lastPageId: String = jsonObject.getJSONObject("page").getString("last_page_id")
          if (lastPageId == null) {
            collector.collect(jsonObject)
          }
        } catch {
          case exception: Exception => exception.printStackTrace(); println(t + "是脏数据")
        }
      }
    })

    //TODO 4.按照min分组
    val keyedStream: KeyedStream[JSONObject, String] = jsonObjectDS.keyBy(_.getJSONObject("common").getString("mid"))

    //TODO 5.使用状态编程实现按照min分组去重
    val uvDS: DataStream[JSONObject] = keyedStream.filter(filter = new RichFilterFunction[JSONObject] {
      var lastVisitState: ValueState[String] = _ //存放过去日期


      override def open(parameters: Configuration): Unit = {
        val stateDescriptor = new ValueStateDescriptor[String]("last-visit", classOf[String])
        // 设置状态TTL
        new StateTtlConfig.Builder(Time.days(1))
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          .build()
        lastVisitState = getRuntimeContext.getState(stateDescriptor)
      }

      override def filter(t: JSONObject): Boolean = {
        val lastDate: String = lastVisitState.value()
        val ts: lang.Long = t.getLong("ts")
        val curDate: String = DateFormatUtil.toDate(ts)

        if (lastDate == null || !curDate.equals(lastDate)) {
          lastVisitState.update(curDate)
          return true
        } else {
          return false
        }
      }
    })

    //TODO 6.将数据写入kafka中
    val targetTopic = "dwd_traffic_unique_visitor_detail"
    uvDS.print(">>>>>>>>>>>>")
    uvDS.map(_.toJSONString)
      .addSink(MyKakfaUtil.getFlinkKafkaProducer(targetTopic))

    //TODO 7.执行程序
    env.execute("DwdTrafficUniqueVisitorDetail")
  }
}
