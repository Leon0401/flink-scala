package com.stark.flink.util

import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @auther leon
  * @create 2019/5/8 16:26
  */
object MyRedisUtil {
  def main(args: Array[String]): Unit = {


  }


  val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()

  def getRedisSink(): RedisSink[(String, String)] = {
    //定义 是何种sink
    new RedisSink[(String, String)](conf, new MyRedisMapper)
  }


  class MyRedisMapper extends RedisMapper[(String, String)] {
    //用何种命令进行保存
    override def getCommandDescription: RedisCommandDescription = {
      // hset   key   field   value
      new RedisCommandDescription(RedisCommand.HSET, "channel_sum")
      // new RedisCommandDescription(RedisCommand.SET  )
    }


    //流中的元素哪部分是key，哪部分是value
    override def getValueFromData(channelSum: (String, String)): String = channelSum._2

    override def getKeyFromData(channelSum: (String, String)): String = channelSum._1
  }

}


