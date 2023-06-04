package com.zhanglei.gmall.realtime.app.dwd.db

import com.zhanglei.gmall.realtime.util.{MyKakfaUtil, MysqlUtil}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import java.time.Duration

//数据流程：web/app -> nginx -> mysql(bin-log) -> Maxwell -> kafka(ods) -> Flinkapp -> kafka(dwd)
//程序流程：Mock -> mysql(bin-log) -> Maxwell -> kafka(zk) -> DwdTradeRefundPaySuc -> kafka(zk)
/** *
 * 退单成功事务事实表
 */
object DwdTradeRefundPaySuc {
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
    //    1.3 设置状态的 TTL(生存时间)(设置为环境的最大乱序程度)
    tableEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(5))

    //TODO 2.读取 topic_db 数据 ,创建 topic_db 表
    tableEnv.executeSql(MyKakfaUtil.getKafkaDB("refund_pay_suc"))

    //TODO 3.建立 Mysql-lookup 表
    tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL)

    //TODO 4.读取退款表数据，筛选退款成功数据
    val refundPayment: Table = tableEnv.sqlQuery(
      """
        |select
        |    `data`['id'] id,
        |    `data`['order_id'] order_id,
        |    `data`['sku_id'] sku_id,
        |    `data`['payment_type'] payment_type,
        |    `data`['callback_time'] callback_time,
        |    `data`['total_amount'] total_amount,
        |    pt
        |from topic_db
        |where `table` = 'refund_payment'
//        |and `data`['refund_status'] = '0702'
        |""".stripMargin)
    tableEnv.createTemporaryView("refund_payment",refundPayment)

    //TODO 5.读取订单数据，筛选退款成功订单
    val orderInfo: Table = tableEnv.sqlQuery(
      """
        |select 
        |    data['id'] id,
        |    data['user_id'] user_id,
        |    data['province_id'] province_id,
        |    `old`
        |from topic_db
        |where `table` = 'order_info'
        |and `type` = 'update'
        |and data['order_status'] = '1006'
        |and `old`['order_status'] is not null
        |""".stripMargin)
    tableEnv.createTemporaryView("order_info",orderInfo)


    //TODO 6.读取退单表，筛选退款成功订单
    val orderRefundInfo: Table = tableEnv.sqlQuery(
      """
        |select
        |    data['order_id'] order_id,
        |    data['sku_id'] sku_id,
        |    data['refund_num'] refund_num,
        |    `old`
        |from topic_db
        |where `table` = 'order_refund_info'
        |and `type` = 'update'
        |and data['refund_status'] = '0705'
        |and `old`['refund_status'] is not null
        |""".stripMargin)
    tableEnv.createTemporaryView("order_refund_info",orderRefundInfo)

    //TODO 7.关联四表
    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    rp.id,
        |    oi.user_id,
        |    rp.order_id,
        |    rp.sku_id,
        |    oi.province_id,
        |    rp.payment_type,
        |    dic.dic_name payment_type_name,
        |    date_format(rp.callback_time,'yyyy-MM-dd'),
        |    rp.callback_time,
        |    ri.refund_num,
        |    rp.total_amount,
        |    current_row_timestamp() row_op_ts
        |from refund_payment rp
        |join order_info oi
        |on rp.order_id = oi.id
        |join order_refund_info ri
        |on rp.order_id = ri.order_id
        |and rp.sku_id = ri.sku_id
        |join base_dic for system_time as of rp.pt as dic
        |on rp.payment_type = dic.dic_code
        |""".stripMargin)
    tableEnv.createTemporaryView("result_table",resultTable)

    //TODO 8.创建退款成功表
    tableEnv.executeSql(
      """
        |create table dwd_trade_refund_pay_suc(
        |    id STRING,
        |    user_id STRING,
        |    order_id STRING,
        |    sku_id STRING,
        |    province_id STRING,
        |    payment_type_code STRING,
        |    payment_type_name STRING,
        |    date_id STRING,
        |    callback_time STRING,
        |    refund_num STRING,
        |    total_amount STRING,
        |    row_op_ts timestamp_ltz(3)
        |)
        |""".stripMargin + MyKakfaUtil.getKafkaSink("dwd_trade_refund_pay_suc"))

    //TODO 9.将数据写入kafka
    tableEnv.executeSql("insert into dwd_trade_refund_pay_suc select * from result_table")
      .print()

    //TODO 10.执行任务
    env.execute("DwdTradeRefundPaySuc")

  }
}
