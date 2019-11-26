package com.ljl.recommend.dataload

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

/**
  * 存储数据到MongoDB和Elasticsearch
  */
object DataLoader {

    private val logger: Logger = LoggerFactory.getLogger(DataLoader.getClass)
    //电影数据
    val MOVIE_DATA_PATH = "D:\\projects\\bigdata-demo\\recommend-demo\\recommend-system\\data-load\\src\\main\\resources\\movies.csv"
    val MONGODB_MOVIE_COLLECTION = "Movie"
    val ES_MOVIE_TYPE = "Movie"

    //评分数据
    val RATING_DATA_PATH = "D:\\projects\\bigdata-demo\\recommend-demo\\recommend-system\\data-load\\src\\main\\resources\\ratings.csv"
    val MONGODB_RATING_COLLECTION = "Rating"

    //标签数据
    val TAG_DATA_PATH = "D:\\projects\\bigdata-demo\\recommend-demo\\recommend-system\\data-load\\src\\main\\resources\\tags.csv"
    val MONGODB_TAG_COLLECTION = "Tag"


    def main(args: Array[String]): Unit = {
        val config = Map(
            "mongo.uri" -> "mongodb://bigdata117:27017/recommender",
            "mongo.db" -> "recommender",
            "es.httpHosts" -> "bigdata116:9200",
            "es.transportHosts" -> "bigdata116:9300,bigdata117:9300,bigdata118:9300",
            "es.index" -> "recommender",
            "es.cluster.name" -> "my-application"
        )

        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
        implicit val eSConfig = ESConfig(config("es.httpHosts"),
            config("es.transportHosts"), config("es.index"), config("es.cluster.name"))

        //1.创建sparksession
        val conf: SparkConf = new SparkConf().setAppName("recommend dataloader").setMaster("local[*]")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._

        //2.加载并清洗数据
        //2.1 加载并处理电影数据,过滤掉用^分割长度等于10的数据并去掉首位空格转换成Movie类型
        val moviesDS = spark.sparkContext.textFile(MOVIE_DATA_PATH, 2)
            .filter(_.split("\\^").length == 10)
            .mapPartitions(lines => {
                lines.map[Movie](line => {
                    val fields = line.split("\\^")
                    Movie(fields(0).trim.toInt, fields(1).trim, fields(2).trim, fields(3).trim, fields(4).trim,
                        fields(5).trim, fields(6).trim, fields(7).trim, fields(8).trim, fields(9).trim)
                })
            }).toDS

        //2.2 加载并处理评分数据
        val ratingsDS = spark.read.csv(RATING_DATA_PATH)
            .map(row => Rating(row.getString(0).toInt, row.getString(1).toInt, row.getString(2).toDouble, row.getString(3).toLong))


        //2.3 加载并处理标签数据
        val tagsDS = spark.read.csv(TAG_DATA_PATH)
            .map(row => Tag(row.getString(0).toInt, row.getString(1).toInt, row.getString(2), row.getString(3).toLong))


        //3.保存数据
        //3.1 保存数据到mongo
        saveDataToMongo(moviesDS, ratingsDS, tagsDS)

        //3.2 把用户给电影打的标签更新moviesDS中，然后保存电影数据到es
        import org.apache.spark.sql.functions._

        //3.2.1 用户给电影打的标签：movieId,tags
        val newTagsDF = tagsDS.groupBy($"movieId")
            .agg(concat_ws("|", collect_set($"tag")).as("tags"))
            .select($"movieId", $"tags")

        //3.2.2 给电影基本信息加上用户给电影打的标签：moviesDS.*,newTagsDF.tags
        val movieWithTagsDF = moviesDS.alias("t1")
            .join(newTagsDF.alias("t2"), $"t1.mid" === $"t2.movieId", "left")
            .select($"t1.*", $"t2.tags")

        //3.2.3 保存电影数据到es
        saveMovieDataToES(movieWithTagsDF)


        //4.关闭sparksession
        spark.stop()
    }


    /**
      * 保存数据到mongo
      *
      * @param moviesDS
      * @param ratingsDS
      * @param tagsDS
      * @param mongoConfig
      */
    def saveDataToMongo(moviesDS: Dataset[Movie],
                        ratingsDS: Dataset[Rating],
                        tagsDS: Dataset[Tag])(implicit mongoConfig: MongoConfig): Unit = {
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
        //删除已存在的数据表
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

        //将数据写入到mongodb
        moviesDS.write
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_MOVIE_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        ratingsDS.write
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_RATING_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        tagsDS.write
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_TAG_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        //对数据表建索引
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("userId" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("movieId" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("userId" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("movieId" -> 1))
        //关闭MongoDB的连接
        mongoClient.close()

    }

    /**
      * 把电影信息保存到es中，搜索用
      *
      * @param movieWithTagsDF
      */
    def saveMovieDataToES(movieWithTagsDF: DataFrame)(implicit eSConfig: ESConfig): Unit = {
        //1. 获取es客户端
        val settings = Settings.builder().put("cluster.name", "my-application").build();
        val esClient = new PreBuiltTransportClient(settings);
        eSConfig.transportHosts.split(",")
            .foreach(tshs => {
                val hosts = tshs.split(":")
                esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hosts(0)), hosts(1).toInt))
            })

        //2. 创建es索引
        initESIndex(esClient, eSConfig.index)
        movieWithTagsDF.write
            .option("es.nodes", eSConfig.httpHosts)
            .option("es.http.timeout", "100m")
            .option("es.mapping.id", "mid")
            .mode("overwrite")
            .format("org.elasticsearch.spark.sql")
            .save(eSConfig.index + "/" + ES_MOVIE_TYPE)

    }

    /**
      * 判断es中是否存在索引
      *
      * @param esClient
      * @param index
      * @return
      */
    def initESIndex(esClient: PreBuiltTransportClient, index: String): Unit = {
        if (!esClient.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists) {
            val createIndexResponse = esClient.admin().indices().prepareCreate(index).get()
            if (createIndexResponse.index() != null) {
                if (logger.isInfoEnabled) {
                    logger.info(s"elasticsearch 索引${index}创建成")
                }
            }
        }
    }
}
