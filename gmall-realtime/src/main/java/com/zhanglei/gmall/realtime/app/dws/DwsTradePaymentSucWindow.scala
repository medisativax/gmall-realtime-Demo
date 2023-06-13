package com.zhanglei.gmall.realtime.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.bean.TradePaymentWindowBean
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyClickHouseUtil, MyKakfaUtil, TimestampLtz3CompareUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector

import java.time.Duration

object DwsTradePaymentSucWindow {
  def main(args: Array[String]): Unit = {
    //TODO 1.获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
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

    //TODO 2.读取DWD层支付主题数据创建流
    val topic = "dwd_trade_pay_detail_suc"
    val groupId = "dws_trade_payment_suc_window"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3.将数据转换成JSON对象
    val jsonObjDS: DataStream[JSONObject] = kafkaDS.flatMap(new FlatMapFunction[String, JSONObject] {
      override def flatMap(t: String, collector: Collector[JSONObject]): Unit = {
        try {
          val jSONObject: JSONObject = JSON.parseObject(t)
          collector.collect(jSONObject)
        } catch {
          case e: Exception => println(">>>>>>>>>>>>" + t)
        }
      }
    })

    //TODO 4.按照订单明细id分组
    val keyedOrderDetailIdDS: KeyedStream[JSONObject, String] = jsonObjDS.keyBy(_.getString("order_detail_id"))

    //TODO 5.使用状态编程保留最新的数据输出
    val filterDS: DataStream[JSONObject] = keyedOrderDetailIdDS.process(new KeyedProcessFunction[String, JSONObject, JSONObject] {
      lazy val valueState: ValueState[JSONObject] = getRuntimeContext.getState(new ValueStateDescriptor[JSONObject]("value-state", classOf[JSONObject]))

      override def processElement(i: JSONObject, context: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
        // 获取状态数据
        val state: JSONObject = valueState.value()

        if (state == null) {
          valueState.update(i)
          // 注册定时器
          context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L)
        } else {
          val stateRt: String = state.getString("row_op_ts")
          val curRt: String = i.getString("row_op_ts")
          val compare: Int = TimestampLtz3CompareUtil.compare(stateRt, curRt)
          if (compare != 1) {
            valueState.update(i)
          }
        }
      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, JSONObject, JSONObject]#OnTimerContext, out: Collector[JSONObject]): Unit = {
        // 输出并清空状态
        out.collect(valueState.value())
        valueState.clear()
      }
    })

    //TODO 6.提取事件时间生成Watermark
    val jsonObjWatermarkDS: DataStream[JSONObject] = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
        override def extractTimestamp(t: JSONObject, l: Long): Long = DateFormatUtil.toTs(t.getString("callback_time"), isFull = true)
      }))

    //TODO 7.按照user_id分组
    val keyedUserIdDS: KeyedStream[JSONObject, String] = jsonObjWatermarkDS.keyBy(_.getString("user_id"))

    //TODO 8.提取独立支付成功用户数
    val tradePaymentDS: DataStream[TradePaymentWindowBean] = keyedUserIdDS.flatMap(new RichFlatMapFunction[JSONObject, TradePaymentWindowBean] {
      lazy val lastDtState = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastDt", classOf[String]))

      override def flatMap(in: JSONObject, collector: Collector[TradePaymentWindowBean]): Unit = {
        //取出数据
        val lastDt: String = lastDtState.value()
        val curDt: String = in.getString("callback_time").split(" ")(0)
        // 定义当日支付人数 & 新增人数
        var paymentSucUniqueUserCount: Long = 0L
        var paymentSucNewUserCount: Long = 0L
        if (lastDt == null) {
          paymentSucUniqueUserCount = 1L
          paymentSucNewUserCount = 1L
          lastDtState.update(curDt)
        } else if (!lastDt.equals(curDt)) {
          paymentSucUniqueUserCount = 1L
          lastDtState.update(curDt)
        }
        // 返回数据
        if (paymentSucUniqueUserCount == 1L) {
          collector.collect(new TradePaymentWindowBean("", "",
            paymentSucUniqueUserCount, paymentSucNewUserCount, 0L))
        }
      }
    })

    //TODO 9.开窗、聚合
    val resultDS: DataStream[TradePaymentWindowBean] = tradePaymentDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce(new ReduceFunction[TradePaymentWindowBean] {
        override def reduce(t: TradePaymentWindowBean, t1: TradePaymentWindowBean): TradePaymentWindowBean = {
          t.paymentSucNewUserCount += t1.paymentSucNewUserCount
          t.paymentSucUniqueUserCount += t1.paymentSucUniqueUserCount
          t
        }
      }, new AllWindowFunction[TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[TradePaymentWindowBean], out: Collector[TradePaymentWindowBean]): Unit = {
          val tradePaymentWindowBean: TradePaymentWindowBean = input.iterator.next()
          tradePaymentWindowBean.stt = DateFormatUtil.toYmdHms(window.getStart)
          tradePaymentWindowBean.edt = DateFormatUtil.toYmdHms(window.getEnd)
          tradePaymentWindowBean.ts = System.currentTimeMillis()
          out.collect(tradePaymentWindowBean)
        }
      })

    //TODO 10.将数据写入到ClickHouse
    resultDS.print(">>>>>>>>>>")
    resultDS.addSink(MyClickHouseUtil.getSinkFunction[TradePaymentWindowBean](
      """
        |insert into dws_trade_payment_suc_window
        | values(?,?,?,?,?)
        |""".stripMargin))

    //TODO 11.启动任务
    env.execute("DwsTradePaymentSucWindow")
  }
}
