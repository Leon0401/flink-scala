package com.stark.flink.bean

/**
  * @auther leon
  * @create 2019/5/9 14:35
  */
case class StartUpLog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      logType: String,
                      vs: String,
                      var logDate: String,
                      var logHour: String,
                      var logHourMinute: String,
                      var ts: Long
                     ) {

}
