package com.zhanglei.gmall.realtime.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.bean.TrafficHomeDetailPageViewBean
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyClickHouseUtil, MyKakfaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Duration

/***
 *  页面浏览各窗口汇总表
 */
object DwsTrafficPageViewWindow {
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

    //TODO 2.读取kafka页面主题数据创建流
    val topic = "dwd_traffic_page_log"
    val groupId = "dws_traffic_page_view_window"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3.将每一行数据转换成JSON对象，过滤出 首页和商品详情页
    val jsonObjectDS: DataStream[JSONObject] = kafkaDS.flatMap(new FlatMapFunction[String, JSONObject] {
      override def flatMap(t: String, collector: Collector[JSONObject]): Unit = {
        val JSONObject: JSONObject = JSON.parseObject(t)
        //获取当前页面id
        val pageId: String = JSONObject.getJSONObject("page").getString("page_id")
        //过滤首页和商品详情页数据
        if ("home".equals(pageId) || "good_detail".equals(pageId)) {
          collector.collect(JSONObject)
        }
      }
    })
    
    //TODO 4.提取事件时间生成Watermark
    val jsonObjectWatermarkDS: DataStream[JSONObject] = jsonObjectDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[JSONObject](Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
        override def extractTimestamp(t: JSONObject, l: Long): Long = t.getLong("ts")
      }))

    //TODO 5.按照Mid分组
    val keyedStreamDS: KeyedStream[JSONObject, String] = jsonObjectWatermarkDS.keyBy(json => json.getJSONObject("common").getString("mid"))

    //TODO 6.使用状态编程求独立访客
    val tranfficHomeDetailDS: DataStream[TrafficHomeDetailPageViewBean] = keyedStreamDS.flatMap(new RichFlatMapFunction[JSONObject, TrafficHomeDetailPageViewBean] {
      var homeLastState: ValueState[String] = _
      var detailLastState: ValueState[String] = _

      override def open(parameters: Configuration): Unit = {
        val homeStateDes: ValueStateDescriptor[String] = new ValueStateDescriptor("home-state", classOf[String])
        val detailStateDes: ValueStateDescriptor[String] = new ValueStateDescriptor("detail-state", classOf[String])

        //设置ttl
        val ttlConfig: StateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          .build()

        homeStateDes.enableTimeToLive(ttlConfig)
        detailStateDes.enableTimeToLive(ttlConfig)

        homeLastState = getRuntimeContext.getState(homeStateDes)
        detailLastState = getRuntimeContext.getState(detailStateDes)
      }

      override def flatMap(in: JSONObject, collector: Collector[TrafficHomeDetailPageViewBean]): Unit = {
        val ts: lang.Long = in.getLong("ts")
        val curDt: String = DateFormatUtil.toDate(ts)
        val homelastDt: String = homeLastState.value()
        val detailLastDt: String = detailLastState.value()
        // 定义访问首页或者详情页的数据
        var homeCt = 0L
        var detailCt = 0L
        // 如果状态为空或者状态时间和当前时间不同，则为需求数据
        if (homelastDt == null || !homelastDt.equals(curDt)) {
          homeCt = 1L
          homeLastState.update(curDt)
        }
        if (detailLastDt == null || !detailLastDt.equals(curDt)) {
          detailCt = 1L
          homeLastState.update(curDt)
        }
        // 满足任何一个数据不为0，则输出
        if (homeCt == 1L || detailCt == 1L) {
          collector.collect(new TrafficHomeDetailPageViewBean("", "",
            homeCt,
            detailCt,
            ts))
        }
      }
    })

    //TODO 7.开窗聚合
    val resultDS: DataStream[TrafficHomeDetailPageViewBean] = tranfficHomeDetailDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
      .reduce(new ReduceFunction[TrafficHomeDetailPageViewBean] {
        override def reduce(t: TrafficHomeDetailPageViewBean, t1: TrafficHomeDetailPageViewBean): TrafficHomeDetailPageViewBean = {
          t.homeUvCt += t1.homeUvCt
          t.goodDetailUvct += t.goodDetailUvct
          t
        }
      }, new AllWindowFunction[TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[TrafficHomeDetailPageViewBean], out: Collector[TrafficHomeDetailPageViewBean]): Unit = {
          val tranfficHomeDetailPageViewBean: TrafficHomeDetailPageViewBean = input.iterator.next()
          tranfficHomeDetailPageViewBean.stt = DateFormatUtil.toYmdHms(window.getStart)
          tranfficHomeDetailPageViewBean.edt = DateFormatUtil.toYmdHms(window.getEnd)
          tranfficHomeDetailPageViewBean.ts = System.currentTimeMillis()
          out.collect(tranfficHomeDetailPageViewBean)
        }
      })

    //TODO 8.将数据写入到ClickHouse中
    resultDS.print(">>>>>>>")
    resultDS.addSink(MyClickHouseUtil.getSinkFunction[TrafficHomeDetailPageViewBean](
      """
        |insert into dws_traffic_page_view_window values(?,?,?,?,?)
        |""".stripMargin))

    //TODO 9.执行任务
    env.execute("DwsTrafficPageViewWindow")
  }
}
