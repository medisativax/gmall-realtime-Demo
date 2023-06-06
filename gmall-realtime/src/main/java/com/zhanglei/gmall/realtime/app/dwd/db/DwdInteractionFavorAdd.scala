package com.zhanglei.gmall.realtime.app.dwd.db

import com.zhanglei.gmall.realtime.util.MyKakfaUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

//数据流程：web/app -> nginx -> mysql(bin-log) -> Maxwell -> kafka(ods) -> Flinkapp -> kafka(dwd)
//程序流程：Mock -> mysql(bin-log) -> Maxwell -> kafka(zookeeper) -> DwdInteractionFavorAdd -> kafka(zookeeper)
/***
 *  收藏商品事务事实表
 */
object DwdInteractionFavorAdd {
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
//    // 1.3 设置状态的 TTL(生存时间)(设置为环境的最大乱序程度)
//    tableEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(5))

    //TODO 2.读取kafka中数据，创建 topic_db 表
    tableEnv.executeSql(MyKakfaUtil.getKafkaDB("dwd_interaction_favor_add"))

    //TODO 3.过滤出收藏商品数据
    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    data['id'] id,
        |    data['user_id'] user_id,
        |    data['sku_id'] sku_id,
        |    date_format(data['create_time'],'yyyy-MM-dd') date_id,
        |    data['create_time']
        |from topic_db
        |where `table` = 'favor_info'
        |and (`type` = 'insert' or (`type` = 'update' and data['is_cancel'] = '0'))
        |""".stripMargin)
    tableEnv.createTemporaryView("result_table",resultTable)

    //TODO 4.创建商品收藏事务事实表
    tableEnv.executeSql(
      """
        |create table dwd_interaction_favor_add(
        |    id STRING,
        |    user_id STRING,
        |    sku_id STRING,
        |    date_id STRING,
        |    date_id STRING,
        |    create_time STRING
        |)
        |""".stripMargin + MyKakfaUtil.getKafkaSink("dwd_interaction_favor_add"))

    //TODO 5.将数据写入kafka
    tableEnv.executeSql("insert into dwd_interaction_favor_add select * from result_table")
      .print()

    //TODO 6.执行任务
    env.execute("DwdInteractionFavorAdd")
  }
}
