package com.zhanglei.gmall.realtime.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.bean.CartAddUuBean
import com.zhanglei.gmall.realtime.util.{DateFormatUtil, MyClickHouseUtil, MyKakfaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction}
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

import java.time.Duration

/***
 *  加购个窗口汇总表
 */
object DwsTradeCartAddUuWindow {
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

    //TODO 2.读取kafka DWD 加购事实表
    val topic = "dwd_trade_cart_add"
    val groupId = "dws_trade_cart_ad_uu_window"
    val kafkaDS: DataStream[String] = env.addSource(MyKakfaUtil.getFlinkKafkaConsumer(topic, groupId))

    //TODO 3.将数据转成JSON对象
    val jsonObjDS: DataStream[JSONObject] = kafkaDS.map(line => JSON.parseObject(line))

    //TODO 4.提取事件时间，生成水位线
    val jsonObjWatermarkDS: DataStream[JSONObject] = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
        override def extractTimestamp(t: JSONObject, l: Long): Long = {
          val operateTime: String = t.getString("operate_time")
          if (operateTime != null){
            DateFormatUtil.toTs(operateTime,isFull = true)
          } else {
            val createTime: String = t.getString("create_time")
            DateFormatUtil.toTs(createTime,isFull = true)
          }
        }
      }))

    //TODO 5.按照user_id分组
    val keyedStreamDS: KeyedStream[JSONObject, String] = jsonObjWatermarkDS.keyBy(json => json.getString("user_id"))

    //TODO 6.使用状态编程提取独立加购用户
    val cartAdduuDS: DataStream[CartAddUuBean] = keyedStreamDS.flatMap(new RichFlatMapFunction[JSONObject, CartAddUuBean] {
      var lastCartAddState: ValueState[String] = _

      override def open(parameters: Configuration): Unit = {
        // 设置ttl
        val ttlConfig: StateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          .build()
        val stateDes = new ValueStateDescriptor[String]("last-cartAdd", classOf[String])
        stateDes.enableTimeToLive(ttlConfig)

        lastCartAddState = getRuntimeContext.getState(stateDes)
      }

      override def flatMap(in: JSONObject, collector: Collector[CartAddUuBean]): Unit = {
        // 获取状态数据以及当前状态日期
        val lastDt: String = lastCartAddState.value()
        var curDt: String = null
        val operateTime: String = in.getString("operate_time")

        if (operateTime != null) {
          curDt = operateTime.split(" ")(0)
        } else {
          val createTime: String = in.getString("create_time")
          curDt = createTime.split(" ")(0)
        }

        if (lastDt == null || !curDt.equals(lastDt)) {
          lastCartAddState.update(curDt)
          collector.collect(CartAddUuBean("", "", 1L, DateFormatUtil.toTs(curDt)))
        }
      }
    })

    //TODO 7.开窗、聚合
    val resultDS: DataStream[CartAddUuBean] = cartAdduuDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
      .reduce(new ReduceFunction[CartAddUuBean] {
        override def reduce(t: CartAddUuBean, t1: CartAddUuBean): CartAddUuBean = {
          t.cartAddUuCt += t1.cartAddUuCt
          t
        }
      }, new AllWindowFunction[CartAddUuBean, CartAddUuBean, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[CartAddUuBean], out: Collector[CartAddUuBean]): Unit = {
          val cartAddUuBean: CartAddUuBean = input.iterator.next()
          cartAddUuBean.stt = DateFormatUtil.toYmdHms(window.getStart)
          cartAddUuBean.edt = DateFormatUtil.toYmdHms(window.getEnd)
          cartAddUuBean.ts = System.currentTimeMillis()
          out.collect(cartAddUuBean)
        }
      })

    //TODO 8.将数据写入到CLickhouse
    resultDS.print(">>>>>>>>>>")
    resultDS.addSink(MyClickHouseUtil.getSinkFunction[CartAddUuBean](
      """
        |insert into dws_trade_cart_add_uu_window
        | values(?,?,?,?)
        |""".stripMargin))

    //TODO 9.执行任务
    env.execute("DwsTradeCartAddUuWindow")
  }
}
