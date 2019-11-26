package com.ljl.es;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ESUtilsTest {

    private TransportClient esClient;

    @Before
    public void init() throws UnknownHostException {
        // 1 设置连接的集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        // 2 连接集群
        esClient = new PreBuiltTransportClient(settings);
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("bigdata116"), 9300));
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("bigdata117"), 9300));
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("bigdata118"), 9300));

        // 3 打印集群名称
        System.out.println(esClient.toString());

    }

    @After
    public void close() {
        esClient.close();
    }

    @Test
    public void test() {
        String docJson = "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的搜索服务器\","
                + "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" + "}";

        IndexResponse indexResponse = esClient.prepareIndex("product", "daily_product", "1")
                .setSource(docJson, XContentType.JSON)
                .execute().actionGet();

        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());


    }

    @Test
    public void testDeleteIndex() {
        ESUtils.deleteIndex(esClient, "blog");
    }

    @Test
    public void testCreateIndex() {
        System.out.println(ESUtils.createIndex(esClient, "blog"));
    }

    @Test
    public void testExistsIndex() {
        System.out.println(ESUtils.existsIndex(esClient, "blog"));
    }


}
