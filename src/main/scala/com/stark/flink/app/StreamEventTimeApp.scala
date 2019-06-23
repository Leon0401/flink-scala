package com.stark.flink.app
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * @auther leon
  * @create 2019/5/8 9:49
  *        监控端口输出，一旦端口停止产生数据，这边自动断开。
  *
  *       flink时间：
  *         EventTime:时间发生的时间，也即业务时间。
  *         IngestionTime:进入flink的时间。
  *         ProcessingTime:处理数据的时间。
  *        flink优选EventTime，只有在EventTime不可用时，才会采用ProcessingTime.
  *
  *
  *       按ProcessingTime划分窗口时，使用timeWindow()定义窗口；
  *       按EventTime划分窗口时:
  *             滑动窗口  .window(SlidingEventTimeWindows.of(Time.milliseconds(5000L),Time.milliseconds(1000L))
  *             滚动窗口  .window(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
  *             会话窗口  .window( EventTimeSessionWindows.withGap(Time.milliseconds(5000L))
  *
  *
  *       生产中代码应用：
  *         先设置水位线，再设置窗口。
  *
  *
  *
  *
  */
object StreamEventTimeApp {
  def main(args: Array[String]): Unit = {

    //配置环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //声明使用eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env.socketTextStream("hadoop102",7777)

    import org.apache.flink.api.scala.createTypeInformation
    val textWithTsDStream: DataStream[(String, Long, Int)] = dataStream.map(text => {
      val arr: Array[String] = text.split(" ")
      (arr(0), arr(1).toLong, 1)
    })
    textWithTsDStream.print("detail")
    // 1 告知flink如何获取数据流中的eventTime   2 告知延迟的waterMark
    val textWithEventTimeDStream: DataStream[(String, Long, Int)] = textWithTsDStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(0L)) {
        override def extractTimestamp(element: (String, Long, Int)): Long = {
          element._2
        }
      }).setParallelism(1)

    //滚动窗口
    //每5秒开一个窗口，统计单词个数，  条件触发看窗口大小加延迟。显示数据看真实窗口区间。
   /* val windowDStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textWithEventTimeDStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.milliseconds(5000)))

    val sumDStream: DataStream[(String, Long, Int)] = windowDStream.sum(2)
    sumDStream.print("sumtotal")
    sumDStream.map(_._3).print("sum")*/


    ///滑动窗口   发车看1000，载客看这个车的开车区间内的乘客数
    //textWithEventTimeDStream.keyBy(0).window(SlidingEventTimeWindows.of(Time.milliseconds(5000L),Time.milliseconds(1000L)))


    ///会话窗口
    val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textWithEventTimeDStream.keyBy(0).window( EventTimeSessionWindows.withGap(Time.milliseconds(5000L))   )
    windowStream.sum(2) .print("windows:::").setParallelism(1)


    env.execute()

  }

}
