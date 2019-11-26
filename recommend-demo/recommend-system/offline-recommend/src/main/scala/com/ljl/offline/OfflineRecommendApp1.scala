package com.ljl.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.jblas.DoubleMatrix

import scala.collection.mutable.ArrayBuffer

case class MovieRating(userId: Int, movieId: Int, rate: Double, timestamp: Long)

//电影推荐类
case class Recommendation(movieId: Int, score: Double)

//给用户推荐的电影
case class UserRecs(userId: Int, recs: Seq[Recommendation])

//电影相似度推荐
case class MovieRecs(movieId: Int, recs: Seq[Recommendation])

object OfflineRecommendApp1 {

    val config = Map(
        "mongo.uri" -> "mongodb://bigdata117:27017/recommender",
        "mongo.db" -> "recommender",
        "es.httpHosts" -> "bigdata116:9200",
        "es.transportHosts" -> "bigdata116:9300,bigdata117:9300,bigdata118:9300",
        "es.index" -> "recommender",
        "es.cluster.name" -> "my-application"
    )

    def main(args: Array[String]): Unit = {

        //1. 创建sparksession
        val conf = new SparkConf().setAppName("Offline Recommend App").setMaster("local[*]")
        val spark = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._

        //2. 加载并处理数据
        //电影评分数据
        val ratingDF = spark.read.option("uri", config("mongo.uri"))
            .option("collection", "Rating")
            .format("com.mongodb.spark.sql")
            .load().as[MovieRating]


        //3.模型训练
        //3.1 训练 LFM
        val maxIter = 10 //最大迭代次数
        val regParam = 0.01 //正则化项参数
        val rank = 5 //特征矩阵的秩
        val model = computeAndEvaluateLFM(ratingDF, maxIter, rank, regParam)

        //3.2 计算用户推荐电影列表
        computeUserRecommendItem(model, ratingDF, spark)

        //3.3 计算每个电影相似度最大的前10个电影
        computeMovieSimMatrix(model, spark)

        //4.停止sparksession
        spark.stop()
    }


    /**
      * 根据隐喻义模型计算电影相似度矩阵，并找出每个电影相似度最大的前10个电影,保存到mongo中
      *
      * @param model
      * @param spark
      */
    def computeMovieSimMatrix(model: MatrixFactorizationModel, spark: SparkSession): Unit = {
        //计算两个电影之间的余弦相似度
        def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
            movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
        }
        import spark.implicits._
        //电影特征信息
        val pFeatures = model.productFeatures.map { case (mid, features) => (mid, new DoubleMatrix(features)) }
        //1. 计算每个电影的相似度top10
        val movieRecs = pFeatures.cartesian(pFeatures)
            .filter(item => item._1._1 != item._2._1)
            .mapPartitions(iter =>
                iter.map {
                    case ((mid1, midFeatrue1), (mid2, midFeature2)) =>
                        val simScore = consinSim(midFeatrue1, midFeature2)
                        (mid1, Recommendation(mid2, simScore))
                }
            ).aggregateByKey(ArrayBuffer[Recommendation]())((recList, rec) => {
            recList += rec
            if (recList.size > 10) {
                recList.sortBy(-_.score).take(10)
            } else {
                recList
            }
        }, (recList1, recList2) => {
            recList1 ++= recList2
            recList1.sortWith(_.score > _.score).take(10)
        }).mapPartitions(iter => iter.map(item => MovieRecs(item._1, item._2))).toDF

        //2. 把每个电影最相似的10个电影信息保存到mongo中
        movieRecs.write
            .option("uri", config("mongo.uri"))
            .option("collection", "movie_recs")
            .format("com.mongodb.spark.sql")
            .mode(SaveMode.Overwrite)
            .save()
    }


    /**
      * 根据隐喻义模型计算每个用户对每个商品的评分，
      * 根据评分矩阵计算给用户推荐的50个候选商品，把结果保存到mongodb种
      * 候选商品定义：用户实际没有对该商品评分，并且根据隐喻义模型计算出的评分矩阵种商品评分最高
      *
      * @param model
      * @param ratingDF
      */
    def computeUserRecommendItem(model: MatrixFactorizationModel, ratingDF: Dataset[MovieRating], spark: SparkSession): Unit = {
        //1. 根据隐喻义模型获取每个用户对每个电影的评分
        val userRDD = ratingDF.rdd.mapPartitions(ratings => ratings.map(_.userId)).distinct()
        val movieRDD = ratingDF.rdd.mapPartitions(ratings => ratings.map(_.movieId)).distinct()
        val userMoviesRDD = userRDD.cartesian(movieRDD)

        //用户对电影的评分
        val userMovieRatingRDD = model.predict(userMoviesRDD)

        //2. 为每个用户计算出带推荐的评分前50的电影，用户评过分的电影表示用户已经看过不再推荐
        //用户已经评过分的电影
        val hasRatedMovieRDD = ratingDF.rdd.mapPartitions(mvIter => mvIter.map(r => ((r.userId, r.movieId), r.rate)))
        //隐喻义模型预测的所有电影的评分
        val allRateMovieRDD = userMovieRatingRDD.mapPartitions(uvr => uvr.map(r => ((r.user, r.product), r.rating)))
        //为每个用户推荐的5个电影列表
        import spark.implicits._
        val userMovieTop50 = allRateMovieRDD.leftOuterJoin(hasRatedMovieRDD)
            .filter {
                //过滤出用户未评分的电影信息
                _._2._2 match {
                    case Some(rate) => false
                    case None => true
                }
            }.map {
            case ((userId, movieId), (rating, _)) => (userId, (movieId, rating))
        }.aggregateByKey(ArrayBuffer[Recommendation]())((res, movieInfo) => {
            res += Recommendation(movieInfo._1, movieInfo._2)
            if (res.size > 5) {
                res.sortWith(_.score > _.score)
                res.remove(res.size - 1)
            }
            res
        }, (res1, res2) => {
            res1.appendAll(res2)
            res1 ++= res2
            res1.sortWith(_.score > _.score)
            res1.take(5)
        }).mapPartitions(iter => iter.map(umt => UserRecs(umt._1, umt._2))).toDF

        //3. 把结果保存到mongodb中
        userMovieTop50.write
            .option("uri", config("mongo.uri"))
            .option("collection", "user_recs")
            .format("com.mongodb.spark.sql")
            .mode(SaveMode.Overwrite)
            .save()

    }


    /**
      * 计算，评估 LFM 隐喻义模型
      *
      * @param ratingDF 评分矩阵
      * @param maxIter  最大迭代次数
      * @param rank     隐式因子的数量
      * @param regParam 正则化项
      * @param numBlocks
      * @return
      */
    def computeAndEvaluateLFM(ratingDF: Dataset[MovieRating],
                              maxIter: Int, rank: Int, regParam: Double, numBlocks: Int = 2) = {
        val ratingRDD = ratingDF.rdd.map(r => org.apache.spark.mllib.recommendation.Rating(r.userId, r.movieId, r.rate))
        ALS.train(ratingRDD, rank, maxIter, 0.01)
    }


}
