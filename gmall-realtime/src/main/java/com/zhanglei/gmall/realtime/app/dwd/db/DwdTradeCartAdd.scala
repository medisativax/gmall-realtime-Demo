package com.zhanglei.gmall.realtime.app.dwd.db

import com.zhanglei.gmall.realtime.util.MyKakfaUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableResult}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row


object DwdTradeCartAdd {
  def main(args: Array[String]): Unit = {
    //TODO 1.获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 生产环境中设置为：kafka topic的分区数
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

//    // 1.1 开启Checkpoint (生产环境一定要开启)
//    env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setCheckpointTimeout(10 * 60000L)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) // 设置checkpoint的同时存在的数量
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L)) // 失败：每隔五秒重启一次，总共三次
//    // 1.2 设置状态后端 (生产环境一定要开启)
//    env.setStateBackend(new HashMapStateBackend())
//    env.getCheckpointConfig.setCheckpointStorage("hdfs://hadoop01:8020/gmall/ck")
//    System.setProperty("HADOOP_USER_NAME", "root")

    //TODO 2.使用DDL方式读取 kafka topic中的数据，创建表
    tableEnv.executeSql(MyKakfaUtil.getKafkaDB("cart_add"))

    //TODO 3.过滤出加购数据
    val cartAddTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    `data`['id'] id,
        |    `data`['user_id'] user_id,
        |    `data`['sku_id'] sku_id,
        |    `data`['cart_price'] cart_price,
        |    if(`type` = 'insert',`data`['sku_num'],
        |    cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num,
        |    `data`['img_url'] img_url,
        |    `data`['sku_name'] sku_name,
        |    `data`['is_checked'] is_checked,
        |    `data`['create_time'] create_time,
        |    `data`['operate_time'] operate_time,
        |    `data`['is_ordered'] is_ordered,
        |    `data`['order_time'] order_time,
        |    `data`['source_type'] source_type,
        |    `data`['source_id'] source_id,
        |    pt
        |from topic_db
        |where `database` = 'gmall'
        |and `table` = 'cart_info'
        |and `type` = 'insert'
        |or (`type` = 'update' and
        |    `old`['sku_num'] is not null and
        |    cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int))
        |""".stripMargin)

    //测试
    tableEnv.toAppendStream[Row](cartAddTable)
      .print(">>>>>>>>>")

    //TODO 4.读取mysql的 base_dic 表建立lookup表


    //TODO 5.关联两张表
    //TODO 6.使用DDL创建事实加购表
    //TODO 7.将数据写出
    //TODO 8.执行任务
    env.execute()
  }
}
