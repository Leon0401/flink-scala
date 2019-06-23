package com.stark.flink.app

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

/**
  * @auther leon
  * @create 2019/5/8 9:40
  */
object BatchWC {

  def main(args: Array[String]): Unit = {
    //创建参数工具
    val tool: ParameterTool = ParameterTool.fromArgs(args)

    val inputPath: String = tool.get("input")
    val outputPath: String = tool.get("output")

    // 配置环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


    // 数据源  读文件
    val ds: DataSet[String] = env.readTextFile(inputPath)

    // 隐式转换
    import org.apache.flink.api.scala.createTypeInformation
    val aggsDs: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)



    // 输出
    aggsDs.print()
    // 设置处理并行度，也就是输出位一个文件
    aggsDs.writeAsCsv(outputPath).setParallelism(1)

    // 执行操作，如果不写，会报error: program plan could not be fetched.
    // map -- reduce -- sink
    env.execute()


  }
}



/*  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val ds: DataSet[String] = env.readTextFile("file:///d:/temp/hello.txt")

    import org.apache.flink.api.scala._
    val aggsDStream: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    aggsDStream.print

  }*/
