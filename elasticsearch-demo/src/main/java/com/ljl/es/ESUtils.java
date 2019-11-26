package com.ljl.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ESUtils {

    private static Logger logger = LoggerFactory.getLogger(ESUtils.class);

    /**
     * 模糊查询
     *
     * @param client
     * @param index
     * @param type
     * @param filed
     * @param word
     * @return
     */
    public static List<Map> fuzzyQuery(TransportClient client, String index,
                                       String type, String filed, String word) {
        ArrayList<Map> result = new ArrayList<>();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices(index)
                .setTypes(type)
                .setQuery(QueryBuilders.fuzzyQuery(filed, word))
                .get();
        logger.info("获得结果数:" + searchResponse.getHits().totalHits);
        getSearchResult(result, searchResponse);
        return result;
    }

    /**
     * 词条查询
     *
     * @param client es客户端
     * @param index  索引
     * @param type   类型
     * @param filed  字段名称
     * @param word   要查询的值
     * @return
     */
    public static List<Map> itemQuery(TransportClient client, String index,
                                      String type, String filed, String word) {
        ArrayList<Map> result = new ArrayList<>();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices(index)
                .setTypes(type)
                .setQuery(QueryBuilders.termQuery(filed, word))
                .get();
        logger.info("获得结果数:" + searchResponse.getHits().totalHits);
        getSearchResult(result, searchResponse);
        return result;
    }


    /**
     * 把输入字符串切词之后匹配【full-text search（全文检索）】
     *
     * @param client es客户端
     * @param index  索引
     * @param type   类型
     * @param filed  字段名称
     * @param word   要查询的值
     * @return
     */
    public static List<Map> matchQuery(TransportClient client, String index,
                                       String type, String filed, String word) {
        ArrayList<Map> result = new ArrayList<>();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchQuery(filed, word))
                .get();
        logger.info("获得结果数:" + searchResponse.getHits().totalHits);
        getSearchResult(result, searchResponse);
        return result;
    }

    /**
     * 把输入字符串不进行切词直接匹配【phrase search（短语搜索）】
     *
     * @param client es客户端
     * @param index  索引
     * @param type   类型
     * @param filed  字段名称
     * @param word   要查询的值
     * @return
     */
    public static List<Map> matchPhraseQuery(TransportClient client, String index,
                                             String type, String filed, String word) {
        ArrayList<Map> result = new ArrayList<>();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchPhraseQuery(filed, word))
                .get();
        logger.info("获得结果数:" + searchResponse.getHits().totalHits);
        getSearchResult(result, searchResponse);
        return result;

    }


    /**
     * 通配符查询
     *
     * @param client es客户端
     * @param index  索引
     * @param type   类型
     * @param filed  字段名称
     * @param word   要查询的值
     * @return
     */
    public static List<Map> wildcardQuery(TransportClient client, String index,
                                          String type, String filed, String word) {
        ArrayList<Map> result = new ArrayList<>();
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices(index)
                .setTypes(type)
                .setQuery(QueryBuilders.wildcardQuery(filed, "*" + word + "*"))
                .get();
        logger.info("获得结果数:" + searchResponse.getHits().totalHits);
        getSearchResult(result, searchResponse);
        return result;

    }


    /**
     * 字符串查询文档【在整个文档中搜索关键字】
     *
     * @param client es客户端
     * @param index  索引
     * @param type   类型
     * @param word
     * @return
     */
    public static List<Map> queryStringQuery(TransportClient client, String index,
                                             String type, String word) {
        ArrayList<Map> result = new ArrayList<>();

        SearchResponse searchResponse = client.prepareSearch()
                .setIndices(index)
                .setTypes(type)
                .setQuery(QueryBuilders.queryStringQuery(word))
                .get();
        logger.info("获得结果数:" + searchResponse.getHits().totalHits);
        getSearchResult(result, searchResponse);
        return result;

    }


    /**
     * 获得一个type中得所有数据，查询所有
     *
     * @param client es客户端
     * @param index  索引
     * @param type   类型
     * @return
     */
    public static List<Map> matchAllQuery(TransportClient client, String index, String type) {
        ArrayList<Map> result = new ArrayList<>();
        SearchResponse searchResponse = client.prepareSearch().setIndices(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
        logger.info("获得结果数:" + searchResponse.getHits().totalHits);
        getSearchResult(result, searchResponse);
        return result;
    }

    /**
     * 根据多个id获得多个文档
     *
     * @param client es客户端
     * @param index  索引
     * @param type   类型
     * @param ids    ids
     * @return
     */
    public static List<Map> getMultiDocumentByIds(TransportClient client, String index, String type, String... ids) {
        ArrayList<Map> result = new ArrayList<>();
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add(index, type, ids)
                .get();
        Iterator<MultiGetItemResponse> iter = multiGetItemResponses.iterator();
        while (iter.hasNext()) {
            result.add(iter.next().getResponse().getSourceAsMap());
        }
        logger.info("获得文档个数:" + result.size());
        return result;
    }


    /**
     * 根据id获取文档
     *
     * @param client es客户端
     * @param index  索引
     * @param type   类型
     * @param id     id
     * @return
     */
    public static Map getDocumentById(TransportClient client, String index, String type, String id) {
        GetResponse data = client.prepareGet(index, type, id).get();
        Map<String, Object> result = data.getSourceAsMap();
        return result;
    }

    /**
     * 通过bulk插入文档，提高效率
     *
     * @param client   es客户端
     * @param index    索引
     * @param type     类型
     * @param docJsons 文档列表
     * @return
     */
    public static boolean insertDocumentByBulk(TransportClient client, String index, String type, List<String> docJsons) {

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        return false;
    }

    /**
     * 根据id删除文档
     *
     * @param client es客户端
     * @param index  索引
     * @param type   类型
     * @param id     要删除的文档id
     * @return
     */
    public static boolean deleteDocumentById(TransportClient client, String index, String type, String id) {
        DeleteResponse deleteResponse = client.prepareDelete(index, type, id).get();
        if (deleteResponse.status() == RestStatus.OK) {
            logger.info("删除文档成功:" + deleteResponse.getId());
            return true;
        }
        return false;
    }

    /**
     * 向type中插入document,文档类型必须为json格式
     *
     * @param client  es客户端
     * @param docJson json格式文档
     * @param index   索引名
     * @param type    类型名
     * @param id      文档id,为空则es自动分配
     * @return
     */
    public static boolean insertDocumentByJson(TransportClient client, String docJson, String index, String type, String id) {
        if (id != null && !"".equals(id)) {
            IndexResponse indexResponse = client.prepareIndex(index, type, id)
                    .setSource(docJson, XContentType.JSON)
                    .execute().actionGet();
            if (indexResponse != null) {
                logger.info("文档插入成功:" + indexResponse.getId());
                return true;
            }
        } else {
            IndexResponse indexResponse = client.prepareIndex(index, type)
                    .setSource(docJson, XContentType.JSON)
                    .execute().actionGet();
            if (indexResponse != null) {
                logger.info("文档插入成功:" + indexResponse.getId());
                return true;
            }
        }
        return false;
    }


    /**
     * 删除索引
     *
     * @param client
     * @param index
     */
    public static void deleteIndex(TransportClient client, String index) {
        if (existsIndex(client, index)) {
            client.admin().indices().prepareDelete(index).get();
        }
    }


    public static boolean createIndexWithMapping(TransportClient client, String index) {

        return false;
    }

    /**
     * 创建索引
     *
     * @param client es客户端
     * @param index  索引名称
     * @return
     */
    public static boolean createIndex(TransportClient client, String index) {
        if (existsIndex(client, index)) {
            logger.info("索引:" + index + "已经存在");
            return true;
        } else {
            CreateIndexResponse createIndexResponse = client.admin().indices()
                    .prepareCreate(index).get();
            if (createIndexResponse.index() != null) {
                logger.info("索引:" + index + "已创建");
                return true;
            } else {
                logger.error("索引:" + index + "创建失败");
                return false;
            }
        }

    }

    /**
     * 判断索引是否存在
     *
     * @param client es客户端
     * @param index  索引名称
     * @return
     */
    public static boolean existsIndex(TransportClient client, String index) {
        AdminClient admin = client.admin();
        IndicesExistsRequest req = new IndicesExistsRequest(index);
        IndicesExistsResponse existsResponse = admin.indices().exists(req).actionGet();
        if (existsResponse.isExists()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 把搜索结果封装到map中
     *
     * @param result
     * @param searchResponse
     */
    private static void getSearchResult(List<Map> result, SearchResponse searchResponse) {
        Iterator<SearchHit> iter = searchResponse.getHits().iterator();
        while (iter.hasNext()) {
            result.add(iter.next().getSourceAsMap());
        }
    }

}
