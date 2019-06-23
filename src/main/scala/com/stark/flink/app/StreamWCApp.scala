package com.stark.flink.app

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @auther leon
  * @create 2019/5/8 9:49
  *        监控端口输出，一旦端口停止产生数据，这边自动断开。
  */
object StreamWCApp {
  def main(args: Array[String]): Unit = {

    //配置环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.socketTextStream("hadoop102",7777)

    import org.apache.flink.api.scala.createTypeInformation
    val resultDStream: DataStream[(String, Int)] = dataStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    //resultDStream.print()
    //一行（一个端口）输出
    resultDStream.print().setParallelism(1)

    env.execute()

  }

}
