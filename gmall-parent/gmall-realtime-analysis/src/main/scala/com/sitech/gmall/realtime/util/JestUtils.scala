package com.sitech.gmall.realtime.util

import com.google.gson.GsonBuilder
import com.sitech.gmall.realtime.base.ESDocumentBase
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory, JestResult}
import io.searchbox.core.{Bulk, Index}
import io.searchbox.indices.CreateIndex
import org.json4s.native.Serialization
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object JestUtils {

    private val logger: Logger = LoggerFactory.getLogger(JestUtils.getClass)

    @volatile private var factory: JestClientFactory = null

    /**
      * 创建 ES client 工厂
      *
      * @param serverUris es 集群http连接，eg: http://bigdata116:9200,http://bigdata117:9200
      */
    def getJestClientFactory(serverUris: String) = {

        if (factory == null) {
            synchronized {
                if (factory == null) {
                    factory = new JestClientFactory
                    val httpClientConfig = new HttpClientConfig.Builder(serverUris.split(",").toList)
                        //                        .defaultCredentials(username, pwd)
                        .gson(new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create())
                        .multiThreaded(true)
                        .maxTotalConnection(10)
                        .readTimeout(10000)
                        .build()
                    factory.setHttpClientConfig(httpClientConfig)
                    logger.info(s"创建JestClientFactory成功:$factory")
                }
            }
        }
        factory
    }

    /**
      * 通过bulk[批量]方式幂等的插入文档
      *
      * @param jest
      * @param index  索引名称
      * @param docSeq doc 必须继承 ESDocumentBase
      * @tparam T 文档类型，必须时可以转成json串的对象
      */
    def putDocViaBulk[T <: ESDocumentBase](jest: JestClient,
                                           index: String,
                                           docSeq: Seq[T],
                                           batchSize: Int) = {
        implicit val formats = org.json4s.DefaultFormats
        var bulk = new Bulk.Builder()
        var bulkResult: JestResult = null
        var count = 0L
        for (doc <- docSeq) {
            //scala类转成json字符串
            val docJson = Serialization.write(doc)
            val bulkableIndex = new Index.Builder(docJson).index(index).`type`(index).build()
            bulk.addAction(bulkableIndex)
            count += 1L
            if (count >= batchSize) {
                //发送bulk请求
                bulkResult = jest.execute(bulk.build())
                handleBulkResult(bulkResult)
                count = 0L
                //新建一个bulk
                bulk = new Bulk.Builder
            }
        }
        //如果还有未提交的数据在这里提交
        if (count > 0L) {
            handleBulkResult(jest.execute(bulk.build()))
        }
    }

    /**
      * 处理bulk操作的结果
      *
      * @param bulkResult
      */
    def handleBulkResult(bulkResult: JestResult) = {
        if (bulkResult.isSucceeded) {
            if (logger.isInfoEnabled) {
                logger.info(s"bulk处理成功,${bulkResult.getJsonString}")
            }
        } else {
            logger.error(s"bulk操作时有失败的请求,失败消息如下:\n ${bulkResult.getErrorMessage}")
            throw new RuntimeException("bulk操作时有失败的请求,中止操作")
        }
    }

    /**
      * 创建index，并给index设置mapping
      *
      * @param jest
      * @param index
      * @param mappingsJson
      * @return
      */
    def createIndexWithMapping(jest: JestClient, index: String, mappingsJson: String) = {

        val createIndex = new CreateIndex.Builder(index).settings(mappingsJson).build()
        val result = jest.execute(createIndex)
        if (result.isSucceeded) {
            if (logger.isInfoEnabled())
                logger.info(s"创建索引成功: ${result.getJsonString}")
            true
        } else {
            logger.error(s"创建索引 $index 时发生错误: ${result.getErrorMessage}")
            false
        }

    }


    def main(args: Array[String]): Unit = {
        val serverUris = "bigdata116:9200,bigdata117:9200,bigdata118:9200"
        val factory = JestUtils.getJestClientFactory(serverUris)
        val jest = factory.getObject

    }
}
