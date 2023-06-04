package com.zhanglei.gmall.realtime.app.dwd.db

import com.zhanglei.gmall.realtime.util.{MyKakfaUtil, MysqlUtil}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration
//数据流程：web/app -> nginx -> mysql(bin-log) -> Maxwell -> kafka(ods) -> Flinkapp -> kafka(dwd)
//程序流程：Mock -> mysql(bin-log) -> Maxwell -> kafka(zk) -> DwdTradeOrderRefund -> kafka(zk)
/***
 *  退单订单事务事实表
 */
object DwdTradeOrderRefund {
  def main(args: Array[String]): Unit = {
    //TODO 1.获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    env.setParallelism(1) // 生产环境中设置为：kafka topic的分区数
    // 获取配置对象
    val configuration: Configuration = tableEnv.getConfig.getConfiguration
    // 为表关联时状态中存储的数据设置过期时间
    configuration.setString("table.exec.state.ttl", "5 s")

//    // 1.1 开启Checkpoint (生产环境一定要开启)
//    env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setCheckpointTimeout(10 * 60000L)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) // 设置checkpoint的同时存在的数量
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L)) // 失败：每隔五秒重启一次，总共三次
//    // 1.2 设置状态后端 (生产环境一定要开启)
//    env.setStateBackend(new HashMapStateBackend())
//    env.getCheckpointConfig.setCheckpointStorage("hdfs://hadoop01:8020/gmall/ck")
//    System.setProperty("HADOOP_USER_NAME", "root")
    //1.3 设置状态的 TTL(生存时间)(设置为环境的最大乱序程度)
    tableEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(5))

    //TODO 2.读取topic_db 中的数据
    tableEnv.executeSql(MyKakfaUtil.getKafkaDB("order_refund"))

    //TODO 3.过滤出退单数据
    val orderRefundInfo: Table = tableEnv.sqlQuery(
      """
        |select
        |    `data`['id'] id,
        |    `data`['user_id'] user_id,
        |    `data`['order_id'] order_id,
        |    `data`['sku_id'] sku_id,
        |    `data`['refund_type'] refund_type,
        |    `data`['refund_num'] refund_num,
        |    `data`['refund_amount'] refund_amount,
        |    `data`['refund_reason_type'] refund_reason_type,
        |    `data`['refund_reason_txt'] refund_reason_txt,
        |    `data`['refund_status'] refund_status,
        |    `data`['create_time'] create_time,
        |    `pt`
        |from topic_db
        |where `table` = 'order_refund_info'
        |and `type` = 'insert'
        |""".stripMargin)
    tableEnv.createTemporaryView("order_refund_info",orderRefundInfo)

    //TODO 4.读取订单表数据，筛选退单数据
    val orderInfoRefund: Table = tableEnv.sqlQuery(
      """
        |select
        |    `data`['id'] id,
        |    `data`['province_id'] province_id,
        |    `old`
        |from topic_db
        |where `table` = 'order_info'
        |and `type` = 'update'
        |and data['order_status'] = '1005'
        |and `old`['order_status'] is not null
        |""".stripMargin)
    tableEnv.createTemporaryView("order_info_refund",orderInfoRefund)

    //TODO 5.创建 base_dic lookup表
    tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL)

    //TODO 6.关联三张表
    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    ri.id,
        |    ri.user_id,
        |    ri.order_id,
        |    ri.sku_id,
        |    oi.province_id,
        |    date_format(ri.create_time,'yyyy-MM-dd') data_id,
        |    ri.create_time,
        |    ri.refund_type,
        |    dic.dic_name,
        |    ri.refund_reason_type,
        |    dic2.dic_name,
        |    ri.refund_reason_txt,
        |    ri.refund_num,
        |    ri.refund_amount,
        |    current_row_timestamp() row_op_ts
        |from order_refund_info ri
        |join order_info_refund oi
        |on ri.order_id = oi.id
        |join base_dic FOR SYSTEM_TIME AS OF ri.pt as dic
        |on ri.refund_type = dic.dic_code
        |join base_dic FOR SYSTEM_TIME AS OF ri.pt as dic2
        |on ri.refund_reason_type = dic2.dic_code
        |""".stripMargin)
    tableEnv.createTemporaryView("result_table",resultTable)

    //TODO 7.创建退单事务事实表
    tableEnv.executeSql(
      """
        |create table dwd_trade_order_refund(
        |    id STRING,
        |    user_id STRING,
        |    order_id STRING,
        |    sku_id STRING,
        |    province_id STRING,
        |    data_id STRING,
        |    create_time STRING,
        |    refund_type_code STRING,
        |    refund_type_name STRING,
        |    refund_reason_type_code STRING,
        |    refund_reason_type_name STRING,
        |    refund_reason_txt STRING,
        |    refund_num STRING,
        |    refund_amount STRING,
        |    row_op_ts timestamp_ltz(3)
        |)
        |""".stripMargin + MyKakfaUtil.getKafkaSink("dwd_trade_order_refund"))

    //TODO 8.将数据写出到kafka
    tableEnv.executeSql("insert into dwd_trade_order_refund select * from result_table")
      .print()

    //TODO 9.执行任务
    env.execute("DwdTradeOrderRefund")
  }
}
