package com.zhanglei.gmall.realtime.app.dwd.db

import com.zhanglei.gmall.realtime.util.{MyKakfaUtil, MysqlUtil}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

//数据流程：web/app -> nginx -> mysql(bin-log) -> Maxwell -> kafka(ods) -> Flinkapp -> kafka(dwd)
//程序流程：Mock -> mysql(bin-log) -> Maxwell -> kafka(zookeeper) -> DwdUserRegister -> kafka(zookeeper)
/***
 *  用户注册事务事实表
 */
object DwdUserRegister {
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
    tableEnv.executeSql(MyKakfaUtil.getKafkaDB("dwd_user_register"))

    //TODO 3.读取用户表数据
    val userInfo: Table = tableEnv.sqlQuery(
      """
        |select
        |    data['id'] user_id,
        |    data['create_time'] create_time
        |from topic_db
        |where `table` = 'user_info'
        |and `type` = 'insert'
        |""".stripMargin)
    tableEnv.createTemporaryView("user_info",userInfo)

    //TODO 4.创建用户注册表
    tableEnv.executeSql(
      """
        |create table dwd_user_register(
        |    user_id STRING,
        |    date_id STRING,
        |    create_time STRING
        |)
        |""".stripMargin + MyKakfaUtil.getKafkaSink("dwd_user_register"))

    //TODO 5.将数据写入
    tableEnv.executeSql(
      """
        |insert into dwd_user_register
        | select
        |    user_id,
        |    date_format(create_time,'yyyy-MM-dd') date_id,
        |    create_time
        | from user_info
        |""".stripMargin)
      .print()

    //TODO 6.执行任务
    env.execute("DwdUserRegister")

  }
}
