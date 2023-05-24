package com.zhanglei.gmall.realtime.app.dwd.log

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyKakfaUtil}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import java.lang

// 流程：web/app -> Nginx -> 日志服务器 -> Flume -> kafka(ODS) -> FlinkApp -> kafka(DWD)
// 程序流程：  mock(lg.sh) -> flume(f1.sh) -> kafka(zookeeper) -> BaseLogApp -> kafka(zookeeper)
object BaseLogApp {
  def main(args: Array[String]): Unit = {
    //TODO 1.获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //生产环境下为：kafka中topic的分区数
//    // 1.1 开启Checkpoint (生产环境一定要开启)
//    env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setCheckpointTimeout(10 * 60000L)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) // 设置checkpoint的同时存在的数量
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L)) // 失败：每隔五秒重启一次，总共三次
//    // 1.2 设置状态后端 (生产环境一定要开启)
//    env.setStateBackend(new HashMapStateBackend())
//    env.getCheckpointConfig.setCheckpointStorage("hdfs://hadoop01:8020/gmall/ck")
//    System.setProperty("HADOOP_USER_NAME", "root")

    //TODO 2.消费Kafka topic_log 中的数据 & 创建流
    val topic = "topic_log"
    val groupId = "base_log_app"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3.验证是否是JSON格式 & 将每行数据转换成为JSONobject
    val dirtyTag: OutputTag[String] = new OutputTag[String]("Dirty") {}
    val jsonObjectDS: DataStream[JSONObject] = kafkaDS.process(new ProcessFunction[String, JSONObject] {
      override def processElement(i: String, context: ProcessFunction[String, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
        try {
          val jsonObject: JSONObject = JSON.parseObject(i)
          collector.collect(jsonObject)
        } catch {
          case exception: Exception => context.output(dirtyTag, i)
        }
      }
    })
    // 获取侧输出流，打印脏数据
    val dirtyDS: DataStream[String] = jsonObjectDS.getSideOutput(dirtyTag)
    dirtyDS.print("Dirty>>>>>>>>>>>>")

    //TODO 4.按照Mid分组
    val keyedStreamDS: KeyedStream[JSONObject, String] = jsonObjectDS.keyBy(_.getJSONObject("common").getString("mid"))

    //TODO 5.使用Flink状态编程：判定新老用户
    val jsonObjectWithNewFlag: DataStream[JSONObject] = keyedStreamDS.map(new RichMapFunction[JSONObject, JSONObject] {
      lazy val lastVisitState: ValueState[String] = getRuntimeContext.getState[String](new ValueStateDescriptor[String]("last-visit", classOf[String]))

      override def map(in: JSONObject): JSONObject = {
        // 获取is_new标记、ts 并将 ts 转换成为年月日
        val isNew: String = in.getJSONObject("common").getString("is_new")
        val ts: lang.Long = in.getLong("ts")
        val curDate: String = DateFormatUtil.toDate(ts)
        // 判断是否为新老用户
        val lastDate: String = lastVisitState.value()
        if ("1".equals(isNew)) {
          if (lastDate == null) {
            lastVisitState.update(curDate)
          } else if (!lastDate.equals(curDate)) {
            in.getJSONObject("common").put("is_new", "0")
          }
        } else if (lastDate == null) {
          lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L))
        }
        return in
      }
    })

    //TODO 6.使用侧输出流进行分流操作   页面日志数据放在主流中
    val startTag = new OutputTag[String]("start") {} //启动日志
    val displayTag = new OutputTag[String]("display") {} //曝光日志
    val actionTag = new OutputTag[String]("action") {} //动作日志
    val errorTag = new OutputTag[String]("error") {} //错误日志

    val pageDS: DataStream[String] = jsonObjectWithNewFlag.process(new ProcessFunction[JSONObject, String] {
      override def processElement(i: JSONObject, context: ProcessFunction[JSONObject, String]#Context, collector: Collector[String]): Unit = {
        // 尝试获取错误信息
        val err: String = i.getString("err")
        if (err != null) {
          // 写入error侧输出流
          context.output(errorTag, i.toJSONString)
        }
        // 移除错误信息
        i.remove("err")

        // 尝试获取启动日志
        val start: String = i.getString("start")
        if (start != null) {
          context.output(startTag, i.toJSONString)
        } else {
          // 获取公共信息 & 页面 & 时间戳
          val common: String = i.getString("common")
          val pageId: String = i.getJSONObject("page").getString("page_id")
          val ts: lang.Long = i.getLong("ts")

          // 尝试获取曝光数据
          val displays: JSONArray = i.getJSONArray("displays")
          if (displays != null && displays.size() > 0) {
            // 遍历曝光数据 & 写入display侧输出流
            for (i <- 0 until displays.size()) {
              val display: JSONObject = displays.getJSONObject(i)
              display.put("common", common)
              display.put("pageId", pageId)
              display.put("ts", ts)
              context.output(displayTag, display.toJSONString)
            }
          }

          // 尝试获取动作数据
          val actions: JSONArray = i.getJSONArray("actions")
          if (actions != null && actions.size() > 0) {
            // 遍历曝光数据 & 写入display侧输出流
            for (i <- 0 until actions.size()) {
              val action: JSONObject = actions.getJSONObject(i)
              action.put("common", common)
              action.put("pageId", pageId)
              context.output(actionTag, action.toJSONString)
            }
          }

          // 移除曝光和动作日志 & 写入到页面日志
          i.remove("displays")
          i.remove("actions")
          collector.collect(i.toJSONString)
        }
      }
    })

    //TODO 7.提取各个侧输出流中数据
    val startDS: DataStream[String] = pageDS.getSideOutput(startTag)
    val displayDS: DataStream[String] = pageDS.getSideOutput(displayTag)
    val actionDS: DataStream[String] = pageDS.getSideOutput(actionTag)
    val errorDS: DataStream[String] = pageDS.getSideOutput(errorTag)

    //TODO 8.将数据输入相对应的topic中
    pageDS.print("page\t>>")
    startDS.print("start\t>>")
    displayDS.print("display\t>>")
    actionDS.print("action\t>>")
    errorDS.print("error\t>>")

    val page_topic = "dwd_traffic_page_log"
    val start_topic = "dwd_traffic_start_log"
    val display_topic = "dwd_traffic_display_log"
    val action_topic = "dwd_traffic_action_log"
    val error_topic = "dwd_traffic_error_log"

    pageDS.addSink(MyKakfaUtil.getFlinkKafkaProducer(page_topic))
    startDS.addSink(MyKakfaUtil.getFlinkKafkaProducer(start_topic))
    displayDS.addSink(MyKakfaUtil.getFlinkKafkaProducer(display_topic))
    actionDS.addSink(MyKakfaUtil.getFlinkKafkaProducer(action_topic))
    errorDS.addSink(MyKakfaUtil.getFlinkKafkaProducer(error_topic))
    //TODO 9.执行任务
    env.execute("BaseLogApp")
  }
}
