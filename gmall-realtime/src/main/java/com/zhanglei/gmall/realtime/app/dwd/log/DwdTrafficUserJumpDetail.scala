package com.zhanglei.gmall.realtime.app.dwd.log

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.util.MyKakfaUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import java.util

// 流程：web/app -> Nginx -> 日志服务器 -> Flume -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> flinkapp -> kafka(dwd)
// 程序流程：  mock(lg.sh) -> flume(f1.sh) -> kafka(zookeeper) -> BaseLogApp -> kafka(zookeeper) -> DwdTrafficUserJumpDetail -> kafka(zookeeper)
object DwdTrafficUserJumpDetail {
  def main(args: Array[String]): Unit = {
    //TODO 1.获取执行环境
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

    //TODO 2.读取kafka 页面日志主题数据创建流
    val topic = "dwd_traffic_page_log"
    val groupId = "userjumpdetail"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3.将每行数据转换成为JSON对象
    val jsonObjectDS: DataStream[JSONObject] = kafkaDS.map(new MapFunction[String, JSONObject] {
      override def map(t: String): JSONObject = {
        try {
          val jsonObject: JSONObject = JSON.parseObject(t)
          return jsonObject
        } catch {
          case exception: Exception => exception.printStackTrace(); println(t + "是脏数据")
        }
        return null
      }
    })

    //TODO 4.按照mid分组
    val keyedStream: KeyedStream[JSONObject, String] = jsonObjectDS
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
          override def extractTimestamp(t: JSONObject, l: Long): Long = t.getLong("ts")
        }))
      .keyBy(_.getJSONObject("common").getString("mid"))

    //TODO 5.定义CEP的模式序列
    val pattern: Pattern[JSONObject, JSONObject] = Pattern.begin[JSONObject]("start").where(new SimpleCondition[JSONObject] {
      override def filter(t: JSONObject): Boolean = t.getJSONObject("page").getString("last_page_id") == null
    })
      .times(2) // 默认宽松近邻
      .consecutive() // 严格近邻
      .within(Time.seconds(10))

    //TODO 6.将模式序列作用到流上
    val patternStream: PatternStream[JSONObject] = CEP.pattern(keyedStream, pattern)

    //TODO 7.提取事件(匹配的事件和超时事件)
    val timeoutTag: OutputTag[String] = new OutputTag[String]("timeout"){}
    val selectDS: DataStream[String] = patternStream.select(timeoutTag,
      new PatternTimeoutFunction[JSONObject, String] {
        override def timeout(map: util.Map[String, util.List[JSONObject]], l: Long): String = map.get("start").get(0).toJSONString
      }, new PatternSelectFunction[JSONObject, String] {
        override def select(map: util.Map[String, util.List[JSONObject]]): String = map.get("start").get(0).toJSONString
      })
    val timeoutDS: DataStream[String] = selectDS.getSideOutput(timeoutTag)

    //TODO 8.合并两个时间
    val unionDS: DataStream[String] = selectDS.union(timeoutDS)

    //TODO 9.将数据写入到kafka
    selectDS.print("select>>>>>>>>>>>")
    timeoutDS.print("timeout>>>>>>>>>>>")
    val targetTopic = "dwd_traffic_user_jump_detail"
    unionDS.addSink(MyKakfaUtil.getFlinkKafkaProducer(targetTopic))

    //TODO 10.启动任务
    env.execute("DwdTrafficUserJumpDetail")
  }
}
