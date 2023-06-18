package com.zhanglei.gmall.realtime.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.app.func.DimAsyncAFunction
import com.zhanglei.gmall.realtime.bean.TradeTrademarkCategoryUserSpuOrderBean
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyKakfaUtil, TimestampLtz3CompareUtil}
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


import java.util
import java.util.concurrent.TimeUnit


object DwsTradeUserSpuOrderWindow {
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
    val groupId = "dws_trabe_user_spu_order_window"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3.转换成为JSON对象
    val jsonObjDS: DataStream[JSONObject] = kafkaDS.flatMap(new FlatMapFunction[String, JSONObject] {
      override def flatMap(t: String, collector: Collector[JSONObject]): Unit = {
        try {
          val jsonObject: JSONObject = JSON.parseObject(t)
          collector.collect(jsonObject)
        } catch {
          case e: Exception => println("脏数据>>>>>>>" + t)
        }
      }
    })

    //TODO 4.安装id分组
    val keyedOrderDetailDS: KeyedStream[JSONObject, String] = jsonObjDS.keyBy(_.getString("id"))

    //TODO 5.去重
    val filterDS: DataStream[JSONObject] = keyedOrderDetailDS.process(new KeyedProcessFunction[String, JSONObject, JSONObject] {
      lazy val valueState: ValueState[JSONObject] = getRuntimeContext.getState(new ValueStateDescriptor[JSONObject]("value-state", classOf[JSONObject]))

      override def processElement(i: JSONObject, context: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
        //获取状态
        val state: JSONObject = valueState.value()
        // 去重
        if (state == null) {
          valueState.update(i)
          //注册定时器
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
        out.collect(valueState.value())
        valueState.clear()
      }
    })

    //TODO 6.将数据转成JAVABean对象
    val tradeUserSpuDS: DataStream[TradeTrademarkCategoryUserSpuOrderBean] = filterDS.map(new MapFunction[JSONObject, TradeTrademarkCategoryUserSpuOrderBean] {
      override def map(t: JSONObject): TradeTrademarkCategoryUserSpuOrderBean = {
        val orderIds: util.HashSet[String] = new util.HashSet[String]
        orderIds.add(t.getString("order_id"))
        TradeTrademarkCategoryUserSpuOrderBean.builder()
          .skuId(t.getString("sku_id"))
          .userId(t.getString("user_id"))
          .orderAmount(t.getDouble("split_total_amount"))
          .orderIdSet(orderIds)
          .ts(DateFormatUtil.toTs(t.getString("create_time"), isFull = true))
          .build()
      }
    })

    //TODO 7.关联sku_info维度表，补充spu_id,tm_id,category3_id
    //    tradeUserSpuDS.map(new RichMapFunction[TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean] {
    //
    //      override def open(parameters: _root_.org.apache.flink.configuration.Configuration): Unit = {
    //        //建立Phoenix连接池
    //      }
    //
    //      override def map(in: _root_.com.zhanglei.gmall.realtime.bean.TradeTrademarkCategoryUserSpuOrderBean): _root_.com.zhanglei.gmall.realtime.bean.TradeTrademarkCategoryUserSpuOrderBean = {
    //        //查询维度表数据，将数据封装在JAVABean中
    //        return null
    //      }
    //    })
    val tradeUserSpuWithSkuDS: DataStream[TradeTrademarkCategoryUserSpuOrderBean] = AsyncDataStream.unorderedWait(tradeUserSpuDS,
      new DimAsyncAFunction[TradeTrademarkCategoryUserSpuOrderBean]("DIM_SKU_INFO") {
        override def getkey(input: TradeTrademarkCategoryUserSpuOrderBean): String = input.getSkuId

        override def join(input: TradeTrademarkCategoryUserSpuOrderBean, dimInfo: JSONObject): Unit = {
          input.setSpuId(dimInfo.getString("SPU_ID"))
          input.setTrademarkId(dimInfo.getString("TM_ID"))
          input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"))

        }
      },
      100, TimeUnit.SECONDS)

    tradeUserSpuWithSkuDS.print("tradeUserSpuWithSkuDS>>>>>>")
    //TODO 8.提取事件时间生成Watermark
    //TODO 9.分组、开窗、聚合
    //TODO 10.关联spu,tm,category维度表，补充信息
    //TODO 11.将数据写入ClickHouse中
    //TODO 12.启动任务
    env.execute("DwsTradeUserSpuOrderWindow")
  }
}
