package com.zhanglei.gmall.realtime.app.dwd.db

import com.zhanglei.gmall.realtime.util.{MyKakfaUtil, MysqlUtil}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import java.time.Duration
//数据流程：web/app -> nginx -> mysql(bin-log) -> Maxwell -> kafka(ods) -> Flinkapp -> kafka(dwd) -> Flinkapp -> kafka(dwd)  -> Flinkapp -> kafka(dwd)
//程序流程：Mock -> mysql(bin-log) -> Maxwell -> kafka(zk) -> DwdTradeOrderPerProcess -> kafka(zk) -> DwdTradeOrderDetail -> kafka(zk) -> DwdTradePayDetailSuc -> kafka(zk)
/***
 * 成功支付订单事务事实表
 */
object DwdTradePayDetailSuc {
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
    // 1.3 设置状态的 TTL(生存时间)(设置为环境的最大乱序程度 )
    tableEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(905))  // 15m + 5s

    //TODO 2.读取 Topic_db 数据并过滤出支付成功数据
    tableEnv.executeSql(MyKakfaUtil.getKafkaDB("pay_detail_suc"))
    val paymentInfo: Table = tableEnv.sqlQuery(
      """
        |select
        |    `data`['user_id'] user_id,
        |    `data`['order_id'] order_id,
        |    `data`['payment_type'] payment_type,
        |    `data`['callback_time'] callback_time,
        |    `pt`
        |from topic_db
        |where `table` = 'payment_info'
        |and `type` = 'update'
        |and `data`['payment_status'] = '1602'
        |""".stripMargin)
    tableEnv.createTemporaryView("payment_info",paymentInfo)
    //测试
//    tableEnv.toAppendStream[Row](paymentInfo).print()


    //TODO 3.消费下单主题数据
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
        |    split_total_amount STRING
        |)
        |""".stripMargin + MyKakfaUtil.getKafkaDDL("dwd_trade_order_detail","pay_detail_suc"))
    //测试
//    tableEnv.executeSql("select * from dwd_trade_order_detail").print()

    //TODO 4.读取Mysql Base_dic表
    tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL)

    //TODO 5.三表关联
    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    od.id order_detail_id,
        |    od.order_id,
        |    od.user_id,
        |    od.sku_id,
        |    od.sku_name,
        |    od.province_id,
        |    od.activity_id,
        |    od.activity_rule_id,
        |    od.coupon_id,
        |    pi.payment_type payment_type_code,
        |    dic.dic_name payment_type_name,
        |    pi.callback_time,
        |    od.source_id,
        |    od.source_type_id,
        |    od.source_type_name,
        |    od.sku_num,
        |    od.order_price,
        |    od.split_activity_amount,
        |    od.split_coupon_amount,
        |    od.split_total_amount split_payment_amount
        |from payment_info pi
        |join dwd_trade_order_detail od
        |on pi.order_id = od.order_id
        |join `base_dic` FOR SYSTEM_TIME AS OF pi.pt as dic
        |on pi.payment_type = dic.dic_code
        |""".stripMargin)
    tableEnv.createTemporaryView("result_table",resultTable)
    //测试
//    tableEnv.toRetractStream[Row](resultTable).print()

    //TODO 6.创建kafka 支付成功表
    tableEnv.executeSql(
      """
        |create table dwd_trade_pay_detail_suc(
        |    order_detail_id STRING,
        |    order_id STRING,
        |    user_id STRING,
        |    sku_id STRING,
        |    sku_name STRING,
        |    province_id STRING,
        |    activity_id STRING,
        |    activity_rule_id STRING,
        |    coupon_id STRING,
        |    payment_type_code STRING,
        |    payment_type_name STRING,
        |    callback_time STRING,
        |    source_id STRING,
        |    source_type_id STRING,
        |    source_type_name STRING,
        |    sku_num STRING,
        |    order_price STRING,
        |    split_activity_amount STRING,
        |    split_coupon_amount STRING,
        |    split_payment_amount STRING,
        |    primary key(order_detail_id) not enforced
        |)
        |""".stripMargin + MyKakfaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc"))

    //TODO 7.将数据写出到kakfa
    tableEnv.executeSql("insert into dwd_trade_pay_detail_suc select * from result_table")
      .print()
    //TODO 8.执行任务
    env.execute("DwdTradePayDetailSuc")
  }
}
