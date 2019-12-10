package com.sitech.gmall.realtime.util

import java.net.InetAddress

import com.sitech.gmall.realtime.base.ESDocumentBase
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.{IndicesExistsRequest, IndicesExistsResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.json4s.native.Serialization
import org.slf4j.{Logger, LoggerFactory}

object ESUtils {

    private val logger: Logger = LoggerFactory.getLogger(ESUtils.getClass)

    /**
      * 通过bulk[批量]方式幂等的插入文档
      *
      * @param client
      * @param index  索引名称
      * @param docSeq doc 必须继承 ESDocumentBase
      * @tparam T 文档类型，必须时可以转成json串的对象
      */
    def putDocViaBulk[T <: ESDocumentBase](client: TransportClient,
                                           index: String,
                                           docSeq: Seq[T],
                                           batchSize: Int) = {
        implicit val formats = org.json4s.DefaultFormats
        var count = 0L
        var bulk = client.prepareBulk()
        for (doc <- docSeq) {
            //scala类转成json字符串
            val docJson = Serialization.write(doc)
            bulk.add(new IndexRequest(index, index, doc.id).source(docJson, XContentType.JSON))
            count += 1L
            if (count >= batchSize) {
                //发送bulk请求
                val bulkRes = bulk.get()
                if (bulkRes.hasFailures) {
                    logger.error(s"bulk操作时有失败的请求,失败消息如下:\n${bulkRes.buildFailureMessage()}")
                }
                count = 0L
                //新建一个bulk
                bulk = client.prepareBulk()
                logger.info(s"成功插入 ${count} 条数据")
            }
        }
        //如果还有未提交的数据在这里提交
        if (count > 0L) {
            val bulkRes = bulk.get()
            if (bulkRes.hasFailures) {
                logger.error(s"bulk操作时有失败的请求,失败消息如下:\n${bulkRes.buildFailureMessage()}")
            }
            count = 0L
            logger.info(s"成功插入 ${count} 条数据")
        }

    }

    /**
      * 创建index，并给index设置mapping
      *
      * @param client
      * @param index
      * @param mappingJson
      * @return
      */
    def createIndexWithMapping(client: TransportClient, index: String, mappingJson: String) = {

        val existsRes: IndicesExistsResponse = client.admin().indices()
            .exists(new IndicesExistsRequest(index)).actionGet()
        if (existsRes.isExists) {
            logger.warn(s"索引${index}已经存在")
        } else {
            client.admin().indices().create(new CreateIndexRequest(index)).actionGet()
            logger.info(s"索引${index}成功创建")
        }
        //构建添加mapping请求
        val putMappingReq = Requests.putMappingRequest(index)
            .`type`(index)
            .source(mappingJson, XContentType.JSON)
        client.admin().indices().putMapping(putMappingReq).get()
    }


    /**
      * 获取客户端
      *
      * @param clusterServers es集群客户端
      * @return
      */
    def getClient(clusterServers: String): TransportClient = {
        val settings = Settings.builder().put("cluster.name", "my-application")
            .put("client.transport.sniff", true) //允许动态监控集群节点添加和移除
            .build();
        val client = new PreBuiltTransportClient(settings);
        clusterServers.split(",").foreach(nodeInfo => {
            val pattern = "([^:]*):(\\d+)".r
            nodeInfo match {
                case pattern(host, port) =>
                    client.addTransportAddress(
                        new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt)
                    )
            }
        })
        logger.info(s"成功连接到Elasticsearch客户端:$client")
        client
    }

    def main(args: Array[String]): Unit = {
        val client: TransportClient = null;
        val bulkRequest = client.prepareBulk();


    }

}
