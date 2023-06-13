package com.zhanglei.gmall.realtime.app.dwd.db

import com.zhanglei.gmall.realtime.util.MyKakfaUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

//数据流程：web/app -> nginx -> mysql(bin-log) -> Maxwell -> kafka(ods) -> Flinkapp -> kafka(dwd) -> Flinkapp -> kafka(dwd)
//程序流程：Mock -> mysql(bin-log) -> Maxwell -> kafka(zk) -> DwdTradeOrderPerProcess -> kafka(zk) -> DwdTradeOrderDetail -> kafka(zk)

/**
 *  下单事务事实表
 */
object DwdTradeOrderDetail {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建执行环境
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
//    // 1.3 设置状态的 TTL(生存时间)(设置为环境的最大乱序程度 )
//    tableEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(5))

    //TODO 2.读取kafka订单预处理主题数据创建表
    tableEnv.executeSql(
      """
        |create table dwd_order_pre(
        |    `id` STRING,
        |    `order_id` STRING,
        |    `sku_id` STRING,
        |    `sku_name` STRING,
        |    `order_price` STRING,
        |    `sku_num` STRING,
        |    `create_time` STRING,
        |    `source_type_id` STRING,
        |    `source_type_name` STRING,
        |    `source_id` STRING,
        |    `split_total_amount` STRING,
        |    `split_activity_amount` STRING,
        |    `split_coupon_amount` STRING,
        |    `consignee` STRING,
        |    `consignee_tel` STRING,
        |    `total_amount` STRING,
        |    `order_status` STRING,
        |    `user_id` STRING,
        |    `payment_way` STRING,
        |    `delivery_address` STRING,
        |    `order_comment` STRING,
        |    `out_trade_no` STRING,
        |    `trade_body` STRING,
        |    `create_time` STRING,
        |    `operate_time` STRING,
        |    `expire_time` STRING,
        |    `process_status` STRING,
        |    `tracking_no` STRING,
        |    `parent_order_id` STRING,
        |    `province_id` STRING,
        |    `activity_reduce_amount` STRING,
        |    `coupon_reduce_amount` STRING,
        |    `original_total_amount` STRING,
        |    `feight_fee` STRING,
        |    `feight_fee_reduce` STRING,
        |    `refundable_time` STRING,
        |    `order_detail_activity_id` STRING,
        |    `activity_id` STRING,
        |    `activity_rule_id` STRING,
        |    `order_detail_coupon_id` STRING,
        |    `coupon_id` STRING,
        |    `coupon_use_id` STRING,
        |    `type` STRING,
        |    `old` map<STRING,STRING>,
        |    row_op_ts TIMESTAMP_LTZ(3)
        |)
        |""".stripMargin + MyKakfaUtil.getKafkaDDL("dwd_trade_order_per_process","order_detail"))

    //TODO 3.过滤出下单数据
    val filteredTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    id,
        |    order_id,
        |    user_id,
        |    sku_id,
        |    sku_name,
        |    sku_num,
        |    order_price,
        |    province_id,
        |    activity_id,
        |    activity_rule_id,
        |    coupon_id,
        |    create_time,
        |    source_id,
        |    source_type_id,
        |    source_type_name,
        |    split_activity_amount,
        |    split_coupon_amount,
        |    split_total_amount,
        |    row_op_ts
        |from dwd_order_pre
        |where `type` = 'insert'
        |""".stripMargin)
    tableEnv.createTemporaryView("filtered_Table",filteredTable)

    //TODO 4.创建DWD层下单数据表
    tableEnv.executeSql(
      """
        |create table dwd_trade_order_detail(
        |    id STRING,
        |    order_id STRING,
        |    user_id STRING,
        |    sku_id STRING,
        |    sku_name STRING,
        |    sku_num STRING,
        |    order_price STRING,
        |    province_id STRING,
        |    activity_id STRING,
        |    activity_rule_id STRING,
        |    coupon_id STRING,
        |    create_time STRING,
        |    source_id STRING,
        |    source_type_id STRING,
        |    source_type_name STRING,
        |    split_activity_amount STRING,
        |    split_coupon_amount STRING,
        |    split_total_amount STRING,
        |    row_op_ts TIMESTAMP_LTZ(3)
        |)
        |""".stripMargin + MyKakfaUtil.getKafkaSink("dwd_trade_order_detail"))
    //TODO 5.将数据写出到kafka
    tableEnv.executeSql("insert into dwd_trade_order_detail select * from filtered_Table")
      .print()
    //TODO 6.启动任务
    env.execute("DwdTradeOrderDetail")
  }
}
