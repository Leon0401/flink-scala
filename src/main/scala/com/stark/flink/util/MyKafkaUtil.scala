package com.stark.flink.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}


/**
  * @auther leon
  * @create 2019/5/8 14:28
  */
object MyKafkaUtil {
  val prop = new Properties()

  prop.setProperty("bootstrap.servers","hadoop102:9092")
  prop.setProperty("group.id","gmall")

  def getConsumer(topic:String ):FlinkKafkaConsumer011[String]= {
    //  SimpleStringSchema 每一条数据的结构，普通的字符串。
    val myKafkaConsumer:FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop)
    myKafkaConsumer
  }

  def getProducer(topic:String):FlinkKafkaProducer011[String]={
    val brokerList = "hadoop102:9092"
    new FlinkKafkaProducer011[String](brokerList,topic,new SimpleStringSchema())
  }

}
