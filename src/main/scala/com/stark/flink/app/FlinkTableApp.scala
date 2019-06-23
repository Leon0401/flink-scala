package com.stark.flink.app

import com.alibaba.fastjson.JSON
import com.stark.flink.bean.StartUpLog
import com.stark.flink.util.MyKafkaUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps

/**
  * @auther leon
  * @create 2019/5/10 16:05
  *         flink Table api
  *
  */
object FlinkTableApp {
  def main(args: Array[String]): Unit = {

    /**
      * table api 的简单使用
      */
    def simpleTableApiTest1 = {

      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

      val kafkaConsumer = MyKafkaUtil.getConsumer("GMALL_STARTUP")

      import org.apache.flink.api.scala.createTypeInformation

      val dstream: DataStream[String] = env.addSource(kafkaConsumer)
      //  获取CaseClass流对象
      //  如果流中的数据类型是case class可以直接根据case class的结构生成table
      val startUpLogDStream: DataStream[StartUpLog] = dstream.map(jsonString => {
        JSON.parseObject(jsonString, classOf[StartUpLog])
      })

      //声明table环境对象
      val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
      val startUpLogTable: Table = tableEnv.fromDataStream(startUpLogDStream)
      val appstoreTable: Table = startUpLogTable.filter("ch ='appstore'").select("mid,uid,ts")


      val resultDStream: DataStream[(String, String, Long)] = appstoreTable.toAppendStream[(String, String, Long)]

    }

    def simpleTableAPiTest2: Unit = {
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

      val kafkaConsumer = MyKafkaUtil.getConsumer("GMALL_STARTUP")

      import org.apache.flink.api.scala.createTypeInformation

      val dstream: DataStream[String] = env.addSource(kafkaConsumer)
      //  获取CaseClass流对象
      val startUpLogDStream: DataStream[StartUpLog] = dstream.map(jsonString => {
        JSON.parseObject(jsonString, classOf[StartUpLog])
      })

      //  声明table环境对象
      val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
      val startUpLogTable: Table = tableEnv.fromDataStream(startUpLogDStream)
      //val appstoreTable: Table = startUpLogTable.select("mid,ch,ts").filter("ch ='appstore'")
      val groupedTable: Table = startUpLogTable.groupBy('ch).select('ch, 'ch.count)

      val resultDStream: DataStream[(Boolean, (Long, Long))] = groupedTable.toRetractStream[(Long, Long)]
      //  true表示最新数据，false表示过期的数据
      //  如果只想看最新数据，可以加一个过滤
      val resDStream: DataStream[(Boolean, (Long, Long))] = resultDStream.filter(_._1)
      resDStream.print()
    }


    def complexTableApiTest3:Unit={
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      val kafkaConsumer = MyKafkaUtil.getConsumer("GMALL_STARTUP")

      val dstream: DataStream[String] = env.addSource(kafkaConsumer)
      //  获取CaseClass流对象
      val startUpLogDStream: DataStream[StartUpLog] = dstream.map(jsonString => {
        JSON.parseObject(jsonString, classOf[StartUpLog])
      })

      //  告知watermark和eventTime如何提取
      //  由于之前有其他算子，不好设置并行度为1，否则失去分布式的特性
      // Time.seconds(0L) 水位线的延迟。
      val startUpLogWithEventTimeDStream: DataStream[StartUpLog] = startUpLogDStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StartUpLog](Time.seconds(0L)) {
        override def extractTimestamp(element: StartUpLog): Long = {
          element.ts
        }
      }).setParallelism(1)

      //  声明table环境对象
      val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
      //  'ts.rowtime 声明ts是时间字段，因为ts的位置程序并不知道，所以，需要写出全部字段。
      val startUpTable: Table = tableEnv.fromDataStream(startUpLogWithEventTimeDStream,'mid,'uid,'appid,'area,'os,'ch,'logType,'vs,'logDate,'logHour,'logHourMinute,'ts.rowtime)

      //  Tumble over 10000.millis 每10秒钟窗口滚动一次    api使用了时间窗口，那么时间的字段必须包含在group by中。
      val resultTable: Table = startUpTable.window(Tumble over 10000.millis on 'ts as 'tt).groupBy('ch,'tt).select('ch,'ch.count)

      val resultSqlTable: Table = tableEnv.sqlQuery("select ch,count(ch) from"+startUpTable+" group by ch ,Tumble(ts,interval '10' SECOND)")

      //table转化成流
      val resultDStream: DataStream[(Boolean, (Long, Long))] = resultSqlTable.toRetractStream[(Long, Long)]
      //  true表示最新数据，false表示过期的数据
      //  如果只想看最新数据，可以加一个过滤
      val resDStream: DataStream[(Boolean, (Long, Long))] = resultDStream.filter(_._1)
      resDStream.print()
    }
  }
}
