package com.zhanglei.gmall.realtime.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.app.func.DimAsyncAFunction
import com.zhanglei.gmall.realtime.bean.TradeProvinceOrderWindowBean
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyClickHouseUtil, MyKakfaUtil, TimestampLtz3CompareUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
import java.util.concurrent.TimeUnit

/***
 *  省份粒度下单各窗口汇总表
 */
object DwsTradeProvinceOrderWindow {
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

    //TODO 2.读取kafka DWD 下单主题数据
    val topic = "dwd_trade_order_detail"
    val groupId = "dws_trade_province_order_window"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //    {"id":"24589","order_id":"9296","user_id":"2262","sku_id":"8","sku_name":"Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机",
    //    "sku_num":"2","order_price":"8197.0","province_id":"28","activity_id":"2","activity_rule_id":"4","coupon_id":null,"create_time":"2020-06-20 17:56:25",
    //    "source_id":null,"source_type_id":"2401","source_type_name":"用户查询","split_activity_amount":"500.0","split_coupon_amount":null,"split_total_amount":"15894.0",
    //    "row_op_ts":"2023-06-19 09:56:29.204Z"}

    //TODO 3.将数据转成为JSON对象
    val jsonObjDS: DataStream[JSONObject] = kafkaDS.flatMap(new FlatMapFunction[String, JSONObject] {
      override def flatMap(t: String, collector: Collector[JSONObject]): Unit = {
        try {
          val jsonObject: JSONObject = JSON.parseObject(t)
          collector.collect(jsonObject)
        } catch {
          case e: Exception => println("脏数据>>>" + t)
        }
      }
    })

    //TODO 4.按照订单明细ID分组，去重
    val filterDS: DataStream[JSONObject] = jsonObjDS.keyBy(_.getString("id"))
      .process(new KeyedProcessFunction[String, JSONObject, JSONObject] {
        lazy val valueState: ValueState[JSONObject] = getRuntimeContext.getState(new ValueStateDescriptor[JSONObject]("value-state", classOf[JSONObject]))

        override def processElement(i: JSONObject, context: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
          val state: JSONObject = valueState.value()
          if (state == null) {
            valueState.update(i)
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L)
          } else {
            val lastRt: String = state.getString("row_op_ts")
            val curRt: String = i.getString("row_op_ts")
            if (TimestampLtz3CompareUtil.compare(lastRt, curRt) != 1) {
              valueState.update(i)
            }
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, JSONObject, JSONObject]#OnTimerContext, out: Collector[JSONObject]): Unit = {
          out.collect(valueState.value())
          valueState.clear()
        }
      })

    //TODO 5.将每行数据转成JAVABean对象
    val tradeProvinceOrderDS: DataStream[TradeProvinceOrderWindowBean] = filterDS.map(new MapFunction[JSONObject, TradeProvinceOrderWindowBean] {
      override def map(t: JSONObject): TradeProvinceOrderWindowBean = {
        import java.util
        val orderIdSets: util.Set[String] = new util.HashSet[String]()
        orderIdSets.add(t.getString("order_id"))

        TradeProvinceOrderWindowBean.builder()
          .provinceId(t.getString("province_id"))
          .orderIdSet(orderIdSets)
          .orderAmount(t.getDouble("split_total_amount"))
          .ts(DateFormatUtil.toTs(t.getString("create_time"), isFull = true))
          .build()
      }
    })

    //TODO 6.提取事件时间生成watermark
    val provinceOrderWithWMDS: DataStream[TradeProvinceOrderWindowBean] = tradeProvinceOrderDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[TradeProvinceOrderWindowBean] {
        override def extractTimestamp(t: TradeProvinceOrderWindowBean, l: Long): Long = t.getTs
      }))

    //TODO 7.分组、开窗、聚合
    val reduceDS: DataStream[TradeProvinceOrderWindowBean] = provinceOrderWithWMDS.keyBy(_.getProvinceId)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce(new ReduceFunction[TradeProvinceOrderWindowBean] {
        override def reduce(t: TradeProvinceOrderWindowBean, t1: TradeProvinceOrderWindowBean): TradeProvinceOrderWindowBean = {
          t.getOrderIdSet.addAll(t1.getOrderIdSet)
          t.setOrderAmount(t.getOrderAmount + t1.getOrderAmount)
          t
        }
      }, new WindowFunction[TradeProvinceOrderWindowBean, TradeProvinceOrderWindowBean, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[TradeProvinceOrderWindowBean], out: Collector[TradeProvinceOrderWindowBean]): Unit = {
          val next: TradeProvinceOrderWindowBean = input.iterator.next()
          next.setStt(DateFormatUtil.toYmdHms(window.getStart))
          next.setEdt(DateFormatUtil.toYmdHms(window.getEnd))
          next.setTs(System.currentTimeMillis())
          next.setOrderCount(next.getOrderIdSet.size())
          out.collect(next)
        }
      })

    //TODO 8.关联省份维度表
    val resultDS: DataStream[TradeProvinceOrderWindowBean] = AsyncDataStream.unorderedWait(reduceDS,
      new DimAsyncAFunction[TradeProvinceOrderWindowBean]("DIM_BASE_PROVINCE") {
        override def getkey(input: TradeProvinceOrderWindowBean): String = input.getProvinceId

        override def join(input: TradeProvinceOrderWindowBean, dimInfo: JSONObject): Unit = input.setProvinceName(dimInfo.getString("NAME"))
      }, 100,
      TimeUnit.SECONDS)

    //TODO 9.将数据写出到ClickHouse
    resultDS.print(">>>>>")
    resultDS.addSink(MyClickHouseUtil.getSinkFunction[TradeProvinceOrderWindowBean](
      """
        |insert into dws_trade_province_order_window
        | values(?,?,?,?,?,?,?)
        |""".stripMargin))

    //TODO 10.启动任务
    env.execute("DwsTradeProvinceOrderWindow")

  }
}
