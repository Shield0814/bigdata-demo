package com.ljl.recommend.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String, shoot: String,
                 language: String, genres: String, actors: String, directors: String)

case class Rating(userId: Int, movieId: Int, rate: Double, timestamp: Long)

case class Tag(userId: Int, movieId: Int, tag: String, timestamp: Long)

/**
  * 统计推荐模块：
  * 根据历史数据统计热门物品进行推荐，比如各分类topN
  */
object StatisticsRecommendApp {
    val config = Map(
        "mongo.uri" -> "mongodb://bigdata117:27017/recommender",
        "mongo.db" -> "recommender",
        "es.httpHosts" -> "bigdata116:9200",
        "es.transportHosts" -> "bigdata116:9300,bigdata117:9300,bigdata118:9300",
        "es.index" -> "recommender",
        "es.cluster.name" -> "my-application"
    )

    def main(args: Array[String]): Unit = {

        val MONGODB_MOVIE_COLLECTION = "Movie"
        val MONGODB_TAG_COLLECTION = "Tag"
        val MONGODB_RATING_COLLECTION = "Rating"
        // 1. 创建sparksession
        val conf = new SparkConf().setAppName("Statistics Recommned App").setMaster("local[*]")
        val spark = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._


        // 2. 加载数据
        //评分数据
        val ratingDF = spark.read.option("uri", config("mongo.uri"))
            .option("collection", MONGODB_RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load().as[Rating]
        ratingDF.cache()

        //电影数据
        val movieDF = spark.read.option("uri", config("mongo.uri"))
            .option("collection", MONGODB_MOVIE_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load().as[Movie]


        // 3. 处理逻辑
        //需求一：统计历史热门电影top100,并保存到mongo中
        computeHistoryHotMovies(ratingDF, spark)

        //需求二: 统计近半年热门电影top50
        computeRecentHotMovies(ratingDF, spark)

        //需求三: 计算每种类型的电影集合中评分最高的10个电影。
        computeHotMoviesWithGenres(movieDF, ratingDF, spark)

        //4. 停止sparksession
        ratingDF.unpersist()
        spark.stop()
    }

    /**
      * 历史热门电影统计
      * 统计电影被评分次数最多的电影TOP100
      *
      * @param ratingDF 电影评分信息
      * @param spark
      * @return 电影评分次数数据(movieId,ratingTimes)
      *
      */
    def computeHistoryHotMovies(ratingDF: Dataset[Rating], spark: SparkSession) = {

        import spark.implicits._
        //电影评分次数DataFrame
        val movieRatingTimesDF = ratingDF.alias("r")
            .groupBy($"r.movieId")
            .agg(
                count($"r.movieId").as("ratingTimes"),
                date_format(current_date(), "yyyyMMdd").as("statDate")
            )

        movieRatingTimesDF
            .orderBy($"ratingTimes".desc)
            .limit(100)
            .select($"r.movieId", $"ratingTimes", $"statDate")
            .write
            .option("uri", config("mongo.uri"))
            .option("collection", "history_hot_movie")
            .mode(SaveMode.Overwrite)
            .format("com.mongodb.spark.sql")
            .save()
    }


    /**
      * 最近热门电影统计
      * 统计近半年被评分电影次数top50
      *
      * @param ratingDF 电影评分信息
      */
    def computeRecentHotMovies(ratingDF: Dataset[Rating], spark: SparkSession): Unit = {
        ratingDF.createOrReplaceTempView("rating")
        val currentDate = "2016-11-01"
        val sql =
            s""" select r.movieId,
               |     count(r.movieId) as ratingTimes,
               |     date_format(current_date(),'yyyyMM') as stat_month
               | from rating r
               | where date_format(from_unixtime(r.timestamp),'yyyyMM') >= date_format(add_months('${currentDate}',-6),'yyyyMM')
               | group by r.movieId
               | order by ratingTimes desc
               | limit 50
            """.stripMargin

        spark.sql(sql)
            .write
            .option("uri", config("mongo.uri"))
            .option("collection", "recent_hot_movie")
            .mode(SaveMode.Overwrite)
            .format("com.mongodb.spark.sql")
            .save()

    }


    /**
      * 每个类别优质电影统计
      * 根据提供的所有电影类别，分别计算每种类型的电影集合中评分最高的10个电影。
      *
      * @param movieDF  电影数据
      * @param ratingDF 评分数据
      */
    def computeHotMoviesWithGenres(movieDF: Dataset[Movie], ratingDF: Dataset[Rating], spark: SparkSession): Unit = {
        movieDF.createOrReplaceTempView("movie_info")
        ratingDF.createOrReplaceTempView("rating_info")

        val sql =
            """select k.* from (
              |  select r1.avg_rate,
              |       explode(split(m.genres,'\\|')) as genre,
              |       rank() over(partition by genre order by r1.avg_rate desc) as rank_no,
              |       m.mid,
              |       m.name,
              |       m.descri,
              |       m.language,
              |       m.directors,
              |       m.issue
              |   from (
              |      select r.movieid,
              |           round(avg(r.rate),2) as avg_rate
              |      from rating_info r
              |      group by r.movieid
              |   ) r1 join movie_info m on m.mid = r1.movieid
              | ) k
              | where k.rank_no <= 10
              | """.stripMargin

        spark.sql(sql)
            .write
            .option("uri", config("mongo.uri"))
            .option("collection", "genre_hot_movie")
            .mode(SaveMode.Overwrite)
            .format("com.mongodb.spark.sql")
            .save()
    }


}
