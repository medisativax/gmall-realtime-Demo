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
}
