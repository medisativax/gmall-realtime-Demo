package com.zhanglei.gmall.realtime.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.bean.TrafficPageViewBean
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyClickHouseUtil, MyKakfaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

// 程序流程：  mock(lg.sh) -> flume(f1.sh) -> kafka(zookeeper) -> BaseLogApp -> kafka(zookeeper) -> DwsTrafficVcChArIsNewPageViewWindow -> ClickHouse(zk)
/** *
 * 版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 */
object DwsTrafficVcChArIsNewPageViewWindow {
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

    //TODO 2.读取三个主题的数据创建流
    val topic: String = "dwd_traffic_page_log"
    val ujdTopic: String = "dwd_traffic_user_jump_detail"
    val uvTopic: String = "dwd_traffic_unique_visitor_detail"
    val groupID: String = "VcChArIsNew_PageView_Window"

    val uvDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(uvTopic, groupID))
    val ujDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(ujdTopic, groupID))
    val pageDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupID))

    //TODO 3.统一数据格式
    val trafficPageViewWithUvDS: DataStream[TrafficPageViewBean] = uvDS.map(fun = line => {
      val jsonObject: JSONObject = JSON.parseObject(line)
      val common: JSONObject = jsonObject.getJSONObject("common")
      TrafficPageViewBean(stt = "", edt = "",
        vc = common.getString("vc"),
        ch = common.getString("ch"),
        ar = common.getString("ar"),
        isNew = common.getString("is_new"),
        uvCt = 1L, svCt = 0L, pvCt = 0L, durSum = 0L, ujCt = 0L,
        ts = jsonObject.getLong("ts"))
    })

    val trafficPageViewWithUjDS: DataStream[TrafficPageViewBean] = ujDS.map(fun = line => {
      val jsonObject: JSONObject = JSON.parseObject(line)
      val common: JSONObject = jsonObject.getJSONObject("common")
      TrafficPageViewBean(stt = "", edt = "",
        vc = common.getString("vc"),
        ch = common.getString("ch"),
        ar = common.getString("ar"),
        isNew = common.getString("is_new"),
        uvCt = 0L, svCt = 0L, pvCt = 0L, durSum = 0L, ujCt = 1L,
        ts = jsonObject.getLong("ts"))
    })

    val trafficPageViewWithPageDS: DataStream[TrafficPageViewBean] = pageDS.map(fun = line => {
      val jsonObject: JSONObject = JSON.parseObject(line)
      val common: JSONObject = jsonObject.getJSONObject("common")
      val page: JSONObject = jsonObject.getJSONObject("page")
      val lastPageId: String = page.getString("last_page_id")
      var svCt = 0L
      if (lastPageId==null){
          svCt = 1L
      }
      TrafficPageViewBean(stt = "", edt = "",
        vc = common.getString("vc"),
        ch = common.getString("ch"),
        ar = common.getString("ar"),
        isNew = common.getString("is_new"),
        uvCt = 0L, svCt = svCt, pvCt = 1L, durSum = page.getLong("during_time"), ujCt = 0L,
        ts = jsonObject.getLong("ts"))
    })

    //TODO 4.将三个流进行 Union
    val unionDS: DataStream[TrafficPageViewBean] = trafficPageViewWithUvDS.union(
      trafficPageViewWithUjDS,
      trafficPageViewWithPageDS
    )

    //TODO 5.提取事件时间并生成时间戳   DwdTrafficUserJumpDetail 延迟十二秒输出，DwsTrafficVcChArIsNewPageViewWindow 延迟两秒关闭窗口，总共延迟关闭十四秒
    val trafficPageViewWithWmDS: DataStream[TrafficPageViewBean] = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[TrafficPageViewBean](Duration.ofSeconds(14))
      .withTimestampAssigner(new SerializableTimestampAssigner[TrafficPageViewBean] {
        override def extractTimestamp(t: TrafficPageViewBean, l: Long): Long = t.ts
      }))

    //TODO 6.分组开窗聚合
    val windowedStream: WindowedStream[TrafficPageViewBean, (String, String, String, String), TimeWindow] = trafficPageViewWithWmDS.keyBy(new KeySelector[TrafficPageViewBean, (String, String, String, String)] {
      override def getKey(in: TrafficPageViewBean): (String, String, String, String) = (in.ar,
        in.ch, in.isNew, in.vc)
    }).window(TumblingEventTimeWindows.of(Time.seconds(10)))

    val resultStram: DataStream[TrafficPageViewBean] = windowedStream.reduce(new ReduceFunction[TrafficPageViewBean] {
      override def reduce(t: TrafficPageViewBean, t1: TrafficPageViewBean): TrafficPageViewBean = {
        t.svCt += t1.svCt
        t.uvCt += t1.uvCt
        t.ujCt += t1.ujCt
        t.pvCt += t1.pvCt
        t.durSum += t1.durSum
        return t
      }
    }, new WindowFunction[TrafficPageViewBean, TrafficPageViewBean, (String, String, String, String), TimeWindow] {
      override def apply(key: (String, String, String, String), window: TimeWindow, input: Iterable[TrafficPageViewBean], out: Collector[TrafficPageViewBean]): Unit = {
        //获取数据
        val next: TrafficPageViewBean = input.iterator.next()
        //包装信息
        next.stt = DateFormatUtil.toYmdHms(window.getStart)
        next.edt = DateFormatUtil.toYmdHms(window.getEnd)
        //修改TS,当前处理时间
        next.ts = System.currentTimeMillis()
        //输出数据
        out.collect(next)
      }
    })

    //TODO 7.将数据写入CLickHouse
    resultStram.print(">>>>>>>>>")
    resultStram.addSink(MyClickHouseUtil.getSinkFunction[TrafficPageViewBean](
      """
        |insert into dws_traffic_vc_ch_ar_is_new_page_view_window
        | values(?,?,?,?,?,?,?,?,?,?,?,?)
        |""".stripMargin ))

    //TODO 8.执行任务
    env.execute("DwsTrafficVcChArIsNewPageViewWindow")
  }
}
