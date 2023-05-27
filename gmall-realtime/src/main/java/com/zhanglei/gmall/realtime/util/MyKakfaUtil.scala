package com.zhanglei.gmall.realtime.util

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang
import java.util.Properties

object MyKakfaUtil {

  val KAKFA_SERVER = "hadoop01:9092"

  def getFlinkKafkaConsumer(topic: String,groupId: String): FlinkKafkaConsumer[String] ={

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAKFA_SERVER)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId)

    return new FlinkKafkaConsumer[String](
      topic,
      new KafkaDeserializationSchema[String] {
        override def isEndOfStream(t: String): Boolean = false

        override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): String =
          if (consumerRecord == null || consumerRecord.value() == null){
            return null
          }else {
            return new String(consumerRecord.value())
          }

        override def getProducedType: TypeInformation[String] = BasicTypeInfo.STRING_TYPE_INFO
      },
      properties
    )
  }

  def getFlinkKafkaProducer(topic: String): FlinkKafkaProducer[String] = {
    new FlinkKafkaProducer[String](KAKFA_SERVER,
      topic,
      new SimpleStringSchema())
  }

  def getFlinkKafkaProducer(topic: String, defaultTopic:String): FlinkKafkaProducer[String] = {
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAKFA_SERVER)
    new FlinkKafkaProducer[String](defaultTopic,
      new KafkaSerializationSchema[String] {
        override def serialize(t: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          if (t == null){
            return null
          }
          return new ProducerRecord(topic,t.getBytes())
        }
      },properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
  }

  /**
   * Kafka-Source DDL 语句
   * @param topic   数据源主题
   * @param groupId 消费者组
   * @return 拼接好的 Kafka 数据源 DDL 语句
   */
  def getKafkaDDL(topic: String, groupId: String): String ={
    return s"""
      | WITH (
      |  'connector' = 'kafka',
      |  'topic' = '$topic',
      |  'properties.bootstrap.servers' = '$KAKFA_SERVER',
      |  'properties.group.id' = '$groupId',
      |  'scan.startup.mode' = 'earliest-offset',
      |  'format' = 'json'
      |)
      |""".stripMargin

    return " with ('connector' = 'kafka', " +
      " 'topic' = '" + topic + "'," +
      " 'properties.bootstrap.servers' = '" + KAKFA_SERVER + "', " +
      " 'properties.group.id' = '" + groupId + "', " +
      " 'format' = 'json', " +
      " 'scan.startup.mode' = 'group-offsets')";
  }

  /**
   * Kafka-Sink DDL 语句
   * @param topic 输出到 Kafka 的目标主题
   * @return 拼接好的 Kafka-Sink DDL 语句
   */
  def getKafkaSink(topic: String): String ={
    return s"""
         | WITH (
         |  'connector' = 'kafka',
         |  'topic' = '$topic',
         |  'properties.bootstrap.servers' = '$KAKFA_SERVER',
         |  'format' = 'json'
         |)
         |""".stripMargin
  }

  /**
   * Kafka-Sink DDL 语句
   * @param topic 输出到 Kafka 的目标主题
   * @return 拼接好的 Kafka-Sink DDL 语句
   */
  def getKafkaDB(groupId: String): String = {
    return """
      |CREATE TABLE topic_db (
      |  `database` STRING,
      |  `table` STRING,
      |  `type` STRING,
      |  `data` MAP<STRING,STRING>,
      |  `old` MAP<STRING,STRING>,
      |  `pt` AS PROCTIME()
      |)
      |""".stripMargin + getKafkaDDL("topic_db",groupId)
  }

}
