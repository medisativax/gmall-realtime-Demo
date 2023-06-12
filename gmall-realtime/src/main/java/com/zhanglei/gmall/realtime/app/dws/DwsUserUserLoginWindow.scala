package com.zhanglei.gmall.realtime.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.bean.UserLoginBean
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyClickHouseUtil, MyKakfaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Duration

/***
 *  用户登陆各窗口汇总表
 */
object DwsUserUserLoginWindow {
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

    //TODO 2.读取kafka 页面日志主题数据，创建流
    val topic = "dwd_traffic_page_log"
    val groupId = "dws_user_userlogin_window"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3.转换数据为JSON对象，并过滤数据
    val jsonObjectDS: DataStream[JSONObject] = kafkaDS.flatMap(new FlatMapFunction[String, JSONObject] {
      override def flatMap(t: String, collector: Collector[JSONObject]): Unit = {
        val jsonObject: JSONObject = JSON.parseObject(t)
        val lastPageId: String = jsonObject.getJSONObject("page").getString("last_page_id")
        val uid: String = jsonObject.getJSONObject("common").getString("uid")
        // uid不等于空，且lastPageId为空或者lastPageId为 “login”，才是登录数据
        if (uid != null && (lastPageId == null || lastPageId.equals("login"))) {
          collector.collect(jsonObject)
        }
      }
    })

    //TODO 4.提取事件时间，创建 Watermark
    val jsonObjectWatermarkDS: DataStream[JSONObject] = jsonObjectDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
        override def extractTimestamp(t: JSONObject, l: Long): Long = t.getLong("ts")
      }))

    //TODO 5.按照uid分组
    val keyedStreamDS: KeyedStream[JSONObject, String] = jsonObjectWatermarkDS.keyBy(json => json.getJSONObject("common").getString("uid"))

    //TODO 6.使用状态编程获取独立用户和7日回流用户
    val userLoginDS: DataStream[UserLoginBean] = keyedStreamDS.flatMap(new RichFlatMapFunction[JSONObject, UserLoginBean] {
      lazy val lastLoginState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastLogin-state", classOf[String]))

      override def flatMap(in: JSONObject, collector: Collector[UserLoginBean]): Unit = {
        // 获取当前日期以及当前数据日期
        val lastLoginDt = lastLoginState.value()
        val ts: lang.Long = in.getLong("ts")
        val curDt: String = DateFormatUtil.toDate(ts)
        // 定义当日独立访客数和七日回流用户数
        var uv = 0L
        var backUv = 0L
        if (lastLoginDt == null) {
          uv = 1L
          lastLoginState.update(curDt)
        } else if (!lastLoginDt.equals(curDt)) {
          uv = 1L
          lastLoginState.update(curDt)
          if (DateFormatUtil.toTs(curDt) - DateFormatUtil.toTs(lastLoginDt) >= (24 * 60 * 60 * 1000L) * 8) {
            backUv = 1L
          }
        }
        if (uv == 1L) {
          collector.collect(new UserLoginBean("", "",
            backUv, uv,
            ts))
        }

      }
    })

    //TODO 7.开窗、聚合
    val resultDS: DataStream[UserLoginBean] = userLoginDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce(new ReduceFunction[UserLoginBean] {
        override def reduce(t: UserLoginBean, t1: UserLoginBean): UserLoginBean = {
          t.uuCt += t1.uuCt
          t.backCt += t1.backCt
          t
        }
      }, new AllWindowFunction[UserLoginBean, UserLoginBean, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[UserLoginBean], out: Collector[UserLoginBean]): Unit = {
          val userLoginBean: UserLoginBean = input.iterator.next()
          userLoginBean.stt = DateFormatUtil.toYmdHms(window.getStart)
          userLoginBean.edt = DateFormatUtil.toYmdHms(window.getEnd)
          userLoginBean.ts = System.currentTimeMillis()
          out.collect(userLoginBean)
        }
      })

    //TODO 8.将数据写入到CLickHouse
    resultDS.print(">>>>>>>>")
    resultDS.addSink(MyClickHouseUtil.getSinkFunction[UserLoginBean](
      """
        |insert into dws_user_user_login_window
        | values(?,?,?,?,?)
        |""".stripMargin))

    //TODO 9.执行任务
    env.execute("DwsUserUserLoginWindow")
  }
}
