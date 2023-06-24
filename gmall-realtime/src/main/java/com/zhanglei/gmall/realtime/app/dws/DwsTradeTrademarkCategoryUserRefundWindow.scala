package com.zhanglei.gmall.realtime.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.app.func.DimAsyncAFunction
import com.zhanglei.gmall.realtime.bean.TradeTrademarkCategoryUserRefundBean
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyClickHouseUtil, MyKakfaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
import java.util.concurrent.TimeUnit

object DwsTradeTrademarkCategoryUserRefundWindow {
  def main(args: Array[String]): Unit = {
    //TODO 1. 环境准备
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

    //TODO 2. 读取kafka dwd 退单主题表
    val topic = "dwd_trade_order_refund"
    val groupId = "dws_trade_mark_category_user_refund_window"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3. 将数据转换成JAVAbean
    val tradeTmCGUserDS: DataStream[TradeTrademarkCategoryUserRefundBean] = kafkaDS.flatMap(new FlatMapFunction[String, TradeTrademarkCategoryUserRefundBean] {
      override def flatMap(t: String, collector: Collector[TradeTrademarkCategoryUserRefundBean]): Unit = {
        try {
          val jsonObject: JSONObject = JSON.parseObject(t)
          import java.util
          val orderIds = new util.HashSet[String]()
          orderIds.add(jsonObject.getString("order_id"))
          collector.collect(
            TradeTrademarkCategoryUserRefundBean.builder()
              .skuId(jsonObject.getString("sku_id"))
              .userId(jsonObject.getString("user_id"))
              .orderIdSet(orderIds)
              .ts(DateFormatUtil.toTs(jsonObject.getString("create_time"), isFull = true))
              .build()
          )
        } catch {
          case e: Exception => println("脏数据>>>>" + t)
        }
      }
    })

    //TODO 4. 关联sku_info维度表 补充字段 tm_id,category3_id
    val tradeWithSkuDS: DataStream[TradeTrademarkCategoryUserRefundBean] = AsyncDataStream.unorderedWait(tradeTmCGUserDS,
      new DimAsyncAFunction[TradeTrademarkCategoryUserRefundBean]("DIM_SKU_INFO") {
        override def getkey(input: TradeTrademarkCategoryUserRefundBean): String = input.getSkuId

        override def join(input: TradeTrademarkCategoryUserRefundBean, dimInfo: JSONObject): Unit = {
          input.setTrademarkId(dimInfo.getString("TM_ID"))
          input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"))
        }
      }, 100,
      TimeUnit.SECONDS)

    //TODO 5. 提取事件时间生成水位线
    val tradeWithSkuWithWMDS: DataStream[TradeTrademarkCategoryUserRefundBean] = tradeWithSkuDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[TradeTrademarkCategoryUserRefundBean] {
        override def extractTimestamp(t: TradeTrademarkCategoryUserRefundBean, l: Long): Long = t.getTs
      }))

    //TODO 6. 分组、开窗、聚合
    val reduceDS: DataStream[TradeTrademarkCategoryUserRefundBean] = tradeWithSkuWithWMDS.keyBy(ds => (ds.getUserId, ds.getTrademarkId, ds.getCategory3Id))
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce(new ReduceFunction[TradeTrademarkCategoryUserRefundBean] {
        override def reduce(t: TradeTrademarkCategoryUserRefundBean, t1: TradeTrademarkCategoryUserRefundBean): TradeTrademarkCategoryUserRefundBean = {
          t.getOrderIdSet.addAll(t1.getOrderIdSet)
          t
        }
      }, new WindowFunction[TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, (String, String, String), TimeWindow] {
        override def apply(key: (String, String, String), window: TimeWindow, input: Iterable[TradeTrademarkCategoryUserRefundBean], out: Collector[TradeTrademarkCategoryUserRefundBean]): Unit = {
          val next: TradeTrademarkCategoryUserRefundBean = input.iterator.next()
          next.setStt(DateFormatUtil.toYmdHms(window.getStart))
          next.setEdt(DateFormatUtil.toYmdHms(window.getEnd))
          next.setTs(System.currentTimeMillis())
          next.setRefundCount(next.getOrderIdSet.size())
          out.collect(next)
        }
      })

    //TODO 7. 关联维度表补充分组所需字段
    val reduceWithTMDS: DataStream[TradeTrademarkCategoryUserRefundBean] = AsyncDataStream.unorderedWait(reduceDS,
      new DimAsyncAFunction[TradeTrademarkCategoryUserRefundBean]("DIM_BASE_TRADEMARK") {
        override def getkey(input: TradeTrademarkCategoryUserRefundBean): String = input.getTrademarkId

        override def join(input: TradeTrademarkCategoryUserRefundBean, dimInfo: JSONObject): Unit = {
          input.setTrademarkName(dimInfo.getString("TM_NAME"))
        }
      }, 100,
      TimeUnit.SECONDS)

    val reduceWithTMWithCG3DS: DataStream[TradeTrademarkCategoryUserRefundBean] = AsyncDataStream.unorderedWait(reduceWithTMDS,
      new DimAsyncAFunction[TradeTrademarkCategoryUserRefundBean]("DIM_BASE_CATEGORY3") {
        override def getkey(input: TradeTrademarkCategoryUserRefundBean): String = input.getCategory3Id

        override def join(input: TradeTrademarkCategoryUserRefundBean, dimInfo: JSONObject): Unit = {
          input.setCategory3Id(dimInfo.getString("NAME"))
          input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"))
        }
      }, 100,
      TimeUnit.SECONDS)

    val reduceWithTMWithCG2DS: DataStream[TradeTrademarkCategoryUserRefundBean] = AsyncDataStream.unorderedWait(reduceWithTMWithCG3DS,
      new DimAsyncAFunction[TradeTrademarkCategoryUserRefundBean]("DIM_BASE_CATEGORY2") {
        override def getkey(input: TradeTrademarkCategoryUserRefundBean): String = input.getCategory2Id

        override def join(input: TradeTrademarkCategoryUserRefundBean, dimInfo: JSONObject): Unit = {
          input.setCategory2Name(dimInfo.getString("NAME"))
          input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"))
        }
      },
      100,
      TimeUnit.SECONDS)

    val reduceWithTMWithCG1DS: DataStream[TradeTrademarkCategoryUserRefundBean] = AsyncDataStream.unorderedWait(reduceWithTMWithCG2DS,
      new DimAsyncAFunction[TradeTrademarkCategoryUserRefundBean]("DIM_BASE_CATEGORY1") {
        override def getkey(input: TradeTrademarkCategoryUserRefundBean): String = input.getCategory1Id

        override def join(input: TradeTrademarkCategoryUserRefundBean, dimInfo: JSONObject): Unit = input.setCategory1Name(dimInfo.getString("NAME"))
      },
      100,
      TimeUnit.SECONDS)

    //TODO 8. 将数据写出到ClickHouse
    reduceWithTMWithCG1DS.print(">>>>>")
    reduceWithTMWithCG1DS.addSink(MyClickHouseUtil.getSinkFunction[TradeTrademarkCategoryUserRefundBean](
      """
        |insert into dws_trade_trademark_category_user_refund_window
        | values(?,?,?,?,?,?,?,?,?,?,?,?,?)
        |""".stripMargin))

    //TODO 9. 启动任务
    env.execute("DwsTradeTrademarkCategoryUserRefundWindow")
  }
}
