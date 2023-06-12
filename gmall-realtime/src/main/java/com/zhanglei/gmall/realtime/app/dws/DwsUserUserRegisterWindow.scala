package com.zhanglei.gmall.realtime.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.bean.UserRegisterBean
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyClickHouseUtil, MyKakfaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Duration

/***
 *  用户注册各窗口汇总表
 */
object DwsUserUserRegisterWindow {
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

    //TODO 2.读取kafka DWD 用户注册表
    val topic = "dwd_user_register"
    val groupId = "dws_user_register_window"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3.将每行数据转换成UserRegisterBean对象
    val jsonObjDS: DataStream[UserRegisterBean] = kafkaDS.map(line => {
      val jsonObject: JSONObject = JSON.parseObject(line)
      val ts: lang.Long = DateFormatUtil.toTs(jsonObject.getString("create_time"),isFull = true)
      new UserRegisterBean("", "", 1L, ts)
    })

    //TODO 4.提取时间戳，生成水位线
    val jsonObjWaterMarkDS: DataStream[UserRegisterBean] = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[UserRegisterBean] {
        override def extractTimestamp(t: UserRegisterBean, l: Long): Long = t.ts
      }))

    //TODO 5.开窗、聚合
    val resultDS: DataStream[UserRegisterBean] = jsonObjWaterMarkDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce(new ReduceFunction[UserRegisterBean] {
        override def reduce(t: UserRegisterBean, t1: UserRegisterBean): UserRegisterBean = {
          t.registerCt += 1L
          t
        }
      }, new AllWindowFunction[UserRegisterBean, UserRegisterBean, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[UserRegisterBean], out: Collector[UserRegisterBean]): Unit = {
          val userRegisterBean: UserRegisterBean = input.iterator.next()
          userRegisterBean.stt = DateFormatUtil.toYmdHms(window.getStart)
          userRegisterBean.edt = DateFormatUtil.toYmdHms(window.getEnd)
          userRegisterBean.ts = System.currentTimeMillis()
          out.collect(userRegisterBean)
        }
      })

    //TODO 6.将数据写入ClickHouse
    resultDS.print(">>>>>>>>>>>")
    resultDS.addSink(MyClickHouseUtil.getSinkFunction[UserRegisterBean](
      """
        |insert into dws_user_user_register_window
        | values(?,?,?,?)
        |""".stripMargin))

    //TODO 7.执行任务
    env.execute("DwsUserRegisterWindow")
  }
}
