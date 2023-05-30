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


object DwdTradeOrderPerProcess {
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

    //TODO 2.创建 topic_db 表
    tableEnv.executeSql(MyKakfaUtil.getKafkaDB("order_per_process"))

    //TODO 3.过滤出订单明细数据
    val orderDetailTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    `data`['id'] id,
        |    `data`['order_id'] order_id,
        |    `data`['sku_id'] sku_id,
        |    `data`['sku_name'] sku_name,
        |    `data`['order_price'] order_price,
        |    `data`['sku_num'] sku_num,
        |    `data`['create_time'] create_time,
        |    `data`['source_type'] source_type,
        |    `data`['source_id'] source_id,
        |    `data`['split_total_amount'] split_total_amount,
        |    `data`['split_activity_amount'] split_activity_amount,
        |    `data`['split_coupon_amount'] split_coupon_amount,
        |    pt
        |from topic_db
        |where `database` = 'gmall'
        |and `table` = 'order_detail'
        |and `type` = 'insert'
        |""".stripMargin)
    tableEnv.createTemporaryView("order_detail_table",orderDetailTable)
    // 测试
//    tableEnv.toAppendStream[Row](orderDetailTable).print("orderDetailTable>>>>>")

    //TODO 4.过滤出订单数据
    val orderInfoTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    `data`['id'] id,
        |    `data`['consignee'] consignee,
        |    `data`['consignee_tel'] consignee_tel,
        |    `data`['total_amount'] total_amount,
        |    `data`['order_status'] order_status,
        |    `data`['user_id'] user_id,
        |    `data`['payment_way'] payment_way,
        |    `data`['delivery_address'] delivery_address,
        |    `data`['order_comment'] order_comment,
        |    `data`['out_trade_no'] out_trade_no,
        |    `data`['trade_body'] trade_body,
        |    `data`['create_time'] create_time,
        |    `data`['operate_time'] operate_time,
        |    `data`['expire_time'] expire_time,
        |    `data`['process_status'] process_status,
        |    `data`['tracking_no'] tracking_no,
        |    `data`['parent_order_id'] parent_order_id,
        |    `data`['province_id'] province_id,
        |    `data`['activity_reduce_amount'] activity_reduce_amount,
        |    `data`['coupon_reduce_amount'] coupon_reduce_amount,
        |    `data`['original_total_amount'] original_total_amount,
        |    `data`['feight_fee'] feight_fee,
        |    `data`['feight_fee_reduce'] feight_fee_reduce,
        |    `data`['refundable_time'] refundable_time,
        |    `type`,
        |    `old`
        |from topic_db
        |where `database` = 'gmall'
        |and `table` = 'order_info'
        |and (`type` = 'insert' or `type` = 'update')
        |""".stripMargin)
    tableEnv.createTemporaryView("order_info_table",orderInfoTable)
    //测试
//    tableEnv.toAppendStream[Row](orderInfoTable).print("orderInfoTable>>>>>>>>")

    //TODO 5.过滤出订单明细活动关联数据
    val orderDetailActivityTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    `data`['id'] id,
        |    `data`['order_id'] order_id,
        |    `data`['order_detail_id'] order_detail_id,
        |    `data`['activity_id'] activity_id,
        |    `data`['activity_rule_id'] activity_rule_id,
        |    `data`['sku_id'] sku_id,
        |    `data`['create_time'] create_time
        |from topic_db
        |where `database` = 'gmall'
        |and `table` = 'order_detail_activity'
        |and `type` = 'insert'
        |""".stripMargin)
    tableEnv.createTemporaryView("order_detail_activity",orderDetailActivityTable)
    //测试
//    tableEnv.toAppendStream[Row](orderDetailActivityTable).print("orderDetailActivityTable>>>>>>>")
    //TODO 6.过滤出订单明细购物卷关联数据
    val orderDetailCouponTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    `data`['id'] id,
        |    `data`['order_id'] order_id,
        |    `data`['order_detail_id'] order_detail_id,
        |    `data`['coupon_id'] coupon_id,
        |    `data`['coupon_use_id'] coupon_use_id,
        |    `data`['sku_id'] sku_id,
        |    `data`['create_time'] create_time
        |from topic_db
        |where `database` = 'gmall'
        |and `table` = 'order_detail_coupon'
        |and `type` = 'insert'
        |""".stripMargin)
    tableEnv.createTemporaryView("order_detail_coupon",orderDetailCouponTable)
    //测试
//    tableEnv.toAppendStream[Row](orderDetailCouponTable).print("orderDetailCouponTable>>>>>>>>")

    //TODO 7.创建 base_dic lookup表
    tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL)

    //TODO 8.关联五张表
    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    od.id,
        |    od.order_id,
        |    od.sku_id,
        |    od.sku_name,
        |    od.order_price,
        |    od.sku_num,
        |    od.create_time,
        |    od.source_type source_type_id,
        |    dic.dic_name source_type_name,
        |    od.source_id,
        |    od.split_total_amount,
        |    od.split_activity_amount,
        |    od.split_coupon_amount,
        |    oi.consignee,
        |    oi.consignee_tel,
        |    oi.total_amount,
        |    oi.order_status,
        |    oi.user_id,
        |    oi.payment_way,
        |    oi.delivery_address,
        |    oi.order_comment,
        |    oi.out_trade_no,
        |    oi.trade_body,
        |    oi.operate_time,
        |    oi.expire_time,
        |    oi.process_status,
        |    oi.tracking_no,
        |    oi.parent_order_id,
        |    oi.province_id,
        |    oi.activity_reduce_amount,
        |    oi.coupon_reduce_amount,
        |    oi.original_total_amount,
        |    oi.feight_fee,
        |    oi.feight_fee_reduce,
        |    oi.refundable_time,
        |    oa.id order_detail_activity_id,
        |    oa.activity_id,
        |    oa.activity_rule_id,
        |    oc.id order_detail_coupon_id,
        |    oc.coupon_id,
        |    oc.coupon_use_id,
        |    oi.`type`,
        |    oi.`old`
        |from order_detail_table od
        |join order_info_table oi
        |on od.order_id = oi.id
        |left join order_detail_activity oa
        |on od.id = oa.order_detail_id
        |left join order_detail_coupon oc
        |on od.id = oc.order_detail_id
        |join base_dic FOR SYSTEM_TIME AS OF od.pt as dic
        |on od.source_type = dic.dic_code
        |""".stripMargin)
    tableEnv.createTemporaryView("result_table",resultTable)
    //测试
//    tableEnv.toRetractStream[Row](resultTable).print("resultTable>>>>>>>>")

    //TODO 9.创建 upsert—kafka 表
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
        |    primary key(id) not enforced
        |)
        |""".stripMargin + MyKakfaUtil.getUpsertKafkaDDL("dwd_trade_order_per_process"))

    //TODO 10. 将数据写出
    tableEnv.executeSql("insert into dwd_order_pre select * from result_table")
      .print()
    //TODO 11. 启动任务
    env.execute("DwdTradeOrderPerProcess")
  }
}
