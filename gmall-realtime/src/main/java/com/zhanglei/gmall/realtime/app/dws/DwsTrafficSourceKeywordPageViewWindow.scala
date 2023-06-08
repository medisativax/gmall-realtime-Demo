package com.zhanglei.gmall.realtime.app.dws

import com.zhanglei.gmall.realtime.app.func.SplitFunction
import com.zhanglei.gmall.realtime.bean.KeywordBean
import com.zhanglei.gmall.realtime.util.{MyClickHouseUtil, MyKakfaUtil}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

// 流程：web/app -> Nginx -> 日志服务器 -> Flume -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
// 程序流程：  mock(lg.sh) -> flume(f1.sh) -> kafka(zookeeper) -> BaseLogApp -> kafka(zookeeper) -> DwsTrafficSourceKeywordPageViewWindow -> ClickHouse(zk)
/***
 *  来源关键词粒度页面浏览各窗口汇总表
 */
object DwsTrafficSourceKeywordPageViewWindow {
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

    //TODO 2.使用DDL方式读取 kafka page_log 主题日志，创建表并提取时间戳生成 WaterMark
    val topic = "dwd_traffic_page_log"
    val groupId = "dws_traffic_source_keyword_page_view_window"

    tableEnv.executeSql(
      """
        |create table page_log (
        |    `page` map<STRING,STRING>,
        |    `ts` bigint,
        |    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),
        |    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND
        |)
        |""".stripMargin + MyKakfaUtil.getKafkaDDL(topic,groupId))

    //TODO 3.过滤出搜索数据
    val filterTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    page['item'] item,
        |    rt
        |from page_log
        |where page['last_page_id'] = 'search'
        |and page['item_type'] = 'keyword'
        |and page['item'] is not null
        |""".stripMargin)
    tableEnv.createTemporaryView("filter_table",filterTable)

    //TODO 4.注册 UDTF & 切词
    tableEnv.createTemporarySystemFunction("SplitFunction",classOf[SplitFunction])
    val splitTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    word,
        |    rt
        |from filter_table,
        |LATERAL TABLE(SplitFunction(item))
        |""".stripMargin)
    tableEnv.createTemporaryView("split_table",splitTable)

    //TODO 5.分组、开窗、聚合
    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt,
        |    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt,
        |    'search' source,
        |    word keyword,
        |    count(*) keyword_count,
        |    UNIX_TIMESTAMP()*1000 ts
        |from split_table
        |group by word,TUMBLE(rt, INTERVAL '10' SECOND)
        |""".stripMargin)
    tableEnv.createTemporaryView("result_table",resultTable)

    //TODO 6.将表转化成流
    val keywordBeanDataStream: DataStream[KeywordBean] = tableEnv.toAppendStream[KeywordBean](resultTable)
    keywordBeanDataStream.print(">>>>>>>>>>>")
    //TODO 7.将数据写出 clickhouse
    keywordBeanDataStream.addSink(MyClickHouseUtil.getSinkFunction[KeywordBean](
      """
        |insert into dws_traffic_source_keyword_page_view_window
        | values(?,?,?,?,?,?)
        |""".stripMargin))

    //TODO 8. 启动任务
    env.execute("DwsTrafficSourceKeywordPageViewWindow")
  }
}
