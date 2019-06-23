package com.stark.flink.util

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * @auther leon
  * @create 2019/5/10 9:11
  */
object MyEsUtil {


  def getEsSink(indexName: String): ElasticsearchSink[String] = {
    val hostList: util.List[HttpHost] = new util.ArrayList[HttpHost]
    hostList.add(new HttpHost("hadoop102", 9200, "http"))
    hostList.add(new HttpHost("hadoop103", 9200, "http"))
    hostList.add(new HttpHost("hadoop104", 9200, "http"))

    val esSinkFunc: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {

        //source里必须是结构化的数据，一般是map或者json对象。
        val jsonObject: JSONObject = JSON.parseObject(element)
        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(jsonObject)
        indexer.add(indexRequest)
      }
    }

    val esSinkBuilder = new ElasticsearchSink.Builder[String](hostList, esSinkFunc)

    esSinkBuilder.setBulkFlushMaxActions(10)

    val esSink: ElasticsearchSink[String] = esSinkBuilder.build()

    esSink
  }
}
