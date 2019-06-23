package com.stark.flink.app

import com.alibaba.fastjson.JSON
import com.stark.flink.bean.StartUpLog
import com.stark.flink.util.{MyEsUtil, MyJdbcSink, MyKafkaUtil, MyRedisUtil}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink

/**
  * @auther leon
  * @create 2019/5/8 14:27
  *         启动之前项目的JSONMocker，模拟数据发到kafka，可以顺利消费。
  *
  *
  *         kafka source： kafka消息
  */
object StartupApp {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaConsumer = MyKafkaUtil.getConsumer("GMALL_STARTUP")

    import org.apache.flink.api.scala.createTypeInformation

    val dstream: DataStream[String] = env.addSource(kafkaConsumer)

    // 需求1 把相同渠道的值进行累加
    //  1) 使用sum算子，就是明确相同的key直接累加
    //dstream.map(jsonString=>{JSON.parseObject(jsonString,classOf[StartUpLog])}).map(startUpLog=>(startUpLog.ch,1)).keyBy(0).sum(1)

    //  2)使用reduce，可以自定义不同的聚合逻辑
    /* val startUpLogDStream: DataStream[StartUpLog] = dstream.map(jsonString => {
       JSON.parseObject(jsonString, classOf[StartUpLog])
     })
     val chCountDStream: DataStream[(String, Int)] = startUpLogDStream.map(startUpLog => (startUpLog.ch, 1)).keyBy(0).reduce { (startupLogCount1, startupLogCount2) =>
       val newCount = startupLogCount1._2 + startupLogCount2._2
       (startupLogCount1._1, newCount)
     }*/

    //chCountDStream.print().setParallelism(1)


    // 需求2 把appstore值累加，其他渠道的累加到other
    //流的切分合并   split：相当于拦截器 ，起到加标志的作用；selector：真正去做分流的工作
    /*val startUpLogDStream: DataStream[StartUpLog] = dstream.map(jsonString => {
      JSON.parseObject(jsonString, classOf[StartUpLog])
    })

    val splitDStream: SplitStream[StartUpLog] = startUpLogDStream.split(startupLog => {
      var flag: List[String] = List()
      if (startupLog.ch.equals("appstore")) {
        flag = List("apple")
      } else {
        flag = List("other")
      }
      flag
    })*/

    //val appleDStream: DataStream[StartUpLog] = splitDStream.select("apple")
    //print输出可以添加说明。
    //appleDStream.print("this is apple").setParallelism(1)
    //val otherDStream: DataStream[StartUpLog] = splitDStream.select("other")
    //appleDStream.print("this is other").setParallelism(1)

    //流的合并
    //union 可以操作多个类型相同的流     connect操作两个类型不一定相同的流
    /*val connDStream: ConnectedStreams[StartUpLog, StartUpLog] = appleDStream.connect(otherDStream)

    val allDStream: DataStream[String] = connDStream.map(
      (startuplog1: StartUpLog) => startuplog1.ch,
      (startuplog2: StartUpLog) => startuplog2.ch
    )

    allDStream.print("all").setParallelism(1)*/


    //val unionDStream: DataStream[StartUpLog] = appleDStream.union(otherDStream)
    //unionDStream.print("union")

    //sink  flink的输出只能依靠sink   可以打开kafka消费端测试。

    //unionDStream.map(_.toString).addSink(MyKafkaUtil.getProducer("gmall_union"))


    //把按渠道的统计值保存到redis    hash   key channel_sum   field ch    value count
    /* val countDStream: DataStream[(String, Int)] = dstream.map(jsonString=>{JSON.parseObject(jsonString,classOf[StartUpLog])}).map(startUpLog=>(startUpLog.ch,1)).keyBy(0).sum(1)

     val resultDStream: DataStream[(String, String)] = countDStream.map(chCount => {
       (chCount._1, chCount._2.toString)
     })

     resultDStream.addSink(MyRedisUtil.getRedisSink())*/


    //sink_3  esSink
    /*   val esSink: ElasticsearchSink[String] = MyEsUtil.getEsSink("gmall_startup")


       dstream.addSink(esSink)

       env.execute()
       println("保存成功")*/


    //sink之4 mysqlSink
    //    val startUpLogDStream: DataStream[StartUpLog] = dstream.map(jsonString => {
    //      JSON.parseObject(jsonString, classOf[StartUpLog])
    //    })
    //  需要手动建mysql表
    //    startUpLogDStream.map(startupLog => Array(startupLog.mid, startupLog.uid, startupLog.ch, startupLog.area, startupLog.ts))
    //      .addSink(new MyJdbcSink("insert into z_startup values(?,?,?,?,?)"))


    // 需求2 把相同渠道的值每十秒进行累加
    /* val chKeyedStream: KeyedStream[(String, Int), Tuple] = dstream.map(jsonString => {
       JSON.parseObject(jsonString, classOf[StartUpLog])
     }).map(startUpLog => (startUpLog.ch, 1)).keyBy(0)

     val timeWindowedStream: WindowedStream[(String, Int), Tuple, TimeWindow] = chKeyedStream.timeWindow(Time.seconds(10L), Time.seconds(3L))
     val chSumDStream: DataStream[(String, Int)] = timeWindowedStream.sum(1)

     chSumDStream.print("10秒的sumWindow:")*/

    // 需求3 把相同渠道每10条数据进行累加
    val chKeyedStream: KeyedStream[(String, Int), Tuple] = dstream.map(jsonString => {
      JSON.parseObject(jsonString, classOf[StartUpLog])
    }).map(startUpLog => (startUpLog.ch, 1)).keyBy(0)

    //countWindow(10L,2L)    10是窗口大小，2是显示获取数据的出发条件
    val countWindowedStream: WindowedStream[(String, Int), Tuple, GlobalWindow] = chKeyedStream.countWindow(10L,2L)
    val chCountDStream: DataStream[(String, Int)] = countWindowedStream.sum(1)
    chCountDStream.print("10条数据的countWindow:")
    env.execute()

  }
}