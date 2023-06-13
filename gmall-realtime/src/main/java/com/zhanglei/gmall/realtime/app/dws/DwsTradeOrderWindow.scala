package com.zhanglei.gmall.realtime.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.bean.TradeOrderBean
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyClickHouseUtil, MyKakfaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction, RichFilterFunction, RichMapFunction}
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
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector

import java.time.Duration

/***
 * 交易域下单各窗口汇总表
 */
object DwsTradeOrderWindow {
  def main(args: Array[String]): Unit = {
    //TODO 1.环境搭建
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

    //TODO 2.读取kafka 下单事务事实表 主题数据，创建流
    val topic = "dwd_trade_order_detail"
    val groupId = "dws_trade_order_window"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3.转换每行数据转换成为JSON对象
    val jsonObjDS: DataStream[JSONObject] = kafkaDS.flatMap(new FlatMapFunction[String, JSONObject] {
      override def flatMap(t: String, collector: Collector[JSONObject]): Unit = {
        try {
          val jsonObject: JSONObject = JSON.parseObject(t)
          collector.collect(jsonObject)
        } catch {
          case e: Exception => println("脏数据>>>>>" + t)
        }
      }
    })

    //TODO 4.按照 order_detail_id 分组
    val keyedOrderDetailIdDS: KeyedStream[JSONObject, String] = jsonObjDS.keyBy(_.getString("id"))

    //TODO 5.对 order_detail_id 去重 (不需要left json 右表数据)
    val filterDS: DataStream[JSONObject] = keyedOrderDetailIdDS.filter(new RichFilterFunction[JSONObject] {
      var valueState: ValueState[String] = _

      override def open(parameters: Configuration): Unit = {
        val valueStateDes = new ValueStateDescriptor[String]("distinct-state", classOf[String])
        //设置TTL
        val ttlConfig: StateTtlConfig = new StateTtlConfig.Builder(Time.seconds(5))
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          .build()
        valueStateDes.enableTimeToLive(ttlConfig)

        valueState = getRuntimeContext.getState(valueStateDes)
      }

      override def filter(t: JSONObject): Boolean = {
        // 获取状态数据
        val state: String = valueState.value()
        if (state == null) {
          valueState.update("1")
          true
        } else {
          false
        }
      }
    })

    //TODO 6.提取事件时间生成watermark
    val jsonObjWaterMarkDS: DataStream[JSONObject] = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
        override def extractTimestamp(t: JSONObject, l: Long): Long = DateFormatUtil.toTs(t.getString("create_time"), isFull = true)
      }))

    //TODO 7.按照user_id分组
    val keyedUserIdDS: KeyedStream[JSONObject, String] = jsonObjWaterMarkDS.keyBy(_.getString("user_id"))

    //TODO 8.提取独立下单用户
    val tradeOrderDS: DataStream[TradeOrderBean] = keyedUserIdDS.map(new RichMapFunction[JSONObject, TradeOrderBean] {
      lazy val lastOrderDtState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastOrderDt-state", classOf[String]))

      override def map(in: JSONObject): TradeOrderBean = {
        val lastOrderDt: String = lastOrderDtState.value()
        val curDt: String = in.getString("create_time").split(" ")(0)
        //定义当天 下单人数 & 新用户人数
        var orderUniqueUserCount = 0L
        var orderNewUserCount = 0L
        if (lastOrderDt == null) {
          orderUniqueUserCount = 1L
          orderNewUserCount = 1L
          lastOrderDtState.update(curDt)
        } else if (!lastOrderDt.equals(curDt)) {
          orderUniqueUserCount = 1L
          lastOrderDtState.update(curDt)
        }
        //获取数据
        val skuNum: Integer = in.getInteger("sku_num")
        val orderPrice: Double = in.getDouble("order_price")
        val activityAmount: Double = in.getDouble("split_activity_amount")
        val couponAmount: Double = in.getDouble("split_coupon_amount")

        TradeOrderBean("", "",
          orderUniqueUserCount, orderNewUserCount,
          activityAmount, couponAmount,
          skuNum * orderPrice,
          0L)
      }
    })

    //TODO 9.开窗、聚合
    val resultDS: DataStream[TradeOrderBean] = tradeOrderDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
      .reduce(new ReduceFunction[TradeOrderBean] {
        override def reduce(t: TradeOrderBean, t1: TradeOrderBean): TradeOrderBean = {
          t.orderUniqueUserCount += t1.orderUniqueUserCount
          t.orderNewUserCount += t1.orderNewUserCount
          t.orderActivityReduceAmount += t1.orderActivityReduceAmount
          t.orderCouponReduceAmount += t1.orderCouponReduceAmount
          t.orderOriginalTotalAmount += t1.orderOriginalTotalAmount
          t
        }
      }, new AllWindowFunction[TradeOrderBean, TradeOrderBean, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[TradeOrderBean], out: Collector[TradeOrderBean]): Unit = {
          val tradeOrderBean: TradeOrderBean = input.iterator.next()
          tradeOrderBean.stt = DateFormatUtil.toYmdHms(window.getStart)
          tradeOrderBean.edt = DateFormatUtil.toYmdHms(window.getEnd)
          tradeOrderBean.ts = System.currentTimeMillis()
          out.collect(tradeOrderBean)
        }
      })

    //TODO 10.将数据写入ClickHouse
    resultDS.print(">>>>>>>")
    resultDS.addSink(MyClickHouseUtil.getSinkFunction[TradeOrderBean](
      """
        |insert into dws_trade_order_window
        | values(?,?,?,?,?,?,?,?)
        |""".stripMargin))

    //TODO 11.执行任务
    env.execute("DwsTradeOrderWindow")
  }
}
