package com.stark.flink.util

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * @auther leon
  * @create 2019/5/10 10:16
  */
class MyJdbcSink(sql: String) extends RichSinkFunction[Array[Any]] {

  val driver = "com.mysql.jdbc.Driver"

  val url = "jdbc:mysql://hadoop102:3306/gmall?useSSL=false"

  val username = "root"

  val password = "root123"

  val maxActive = "20"

  var connection: Connection = null;

  //创建连接
  override def open(parameters: Configuration): Unit = {
    val prop = new Properties()
    prop.put("driverClassName", driver)
    prop.put("url", url)
    prop.put("username", username)
    prop.put("password", password)
    prop.put("maxActive", maxActive)

    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(prop)
    connection = dataSource.getConnection()
  }

  //反复调用
  override def invoke(values: Array[Any], context: SinkFunction.Context[_]): Unit = {

    val ps: PreparedStatement = connection.prepareStatement(sql)
    for (i <- 0 until values.length) {
      //设置sql中占位符的对应数据  ?,?,?,?,?
      ps.setObject(i + 1, values(i))
    }
    ps.executeUpdate()
  }

  override def close(): Unit = {
    if (connection != null) {
      connection.close()
    }
  }
}
