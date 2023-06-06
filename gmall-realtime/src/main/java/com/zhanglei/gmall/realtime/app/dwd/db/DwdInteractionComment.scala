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
//程序流程：Mock -> mysql(bin-log) -> Maxwell -> kafka(zookeeper) -> DwdInteractionComment -> kafka(zookeeper)
/** *
 * 评价事务事实表
 */
object DwdInteractionComment {
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
    tableEnv.executeSql(MyKakfaUtil.getKafkaDB("dwd_interaction_comment"))

    //TODO 3.过滤出评价表数据
    val commentInfo: Table = tableEnv.sqlQuery(
      """
        |select
        |    data['id'] id,
        |    data['user_id'] user_id,
        |    data['sku_id'] sku_id,
        |    data['order_id'] order_id,
        |    data['create_time'] create_time,
        |    data['appraise'] appraise,
        |    pt
        |from topic_db
        |where `table` = 'comment_info'
        |and `type` = 'insert'
        |""".stripMargin)
    tableEnv.createTemporaryView("comment_info",commentInfo)

    //TODO 4.创建 base_dic 表
    tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL)

    //TODO 5.关联两表
    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    ci.id,
        |    ci.user_id,
        |    ci.sku_id,
        |    ci.order_id,
        |    date_format(ci.create_time,'yyyy-MM-dd') date_id,
        |    ci.create_time,
        |    ci.appraise,
        |    dic.dic_name
        |from comment_info ci
        |join base_dic for system_time as of ci.pt as dic
        |on ci.appraise = dic.dic_code
        |""".stripMargin)
    tableEnv.createTemporaryView("result_table",resultTable)

    //TODO 6.创建评价事务事实表
    tableEnv.executeSql(
      """
        |create table dwd_interaction_comment(
        |    ida STRING,
        |    user_ida STRING,
        |    sku_ida STRING,
        |    order_ida STRING,
        |    date_ida STRING,
        |    create_timea STRING,
        |    appraise_codea STRING,
        |    appraise_namea STRING
        |)
        |""".stripMargin + MyKakfaUtil.getKafkaSink("dwd_interaction_comment"))

    //TODO 7.将数据写入到kafka中
    tableEnv.executeSql("insert into dwd_interaction_comment select * from result_table")
      .print()

    //TODO 8.执行任务
    env.execute("DwdInteractionComment")
  }
}
