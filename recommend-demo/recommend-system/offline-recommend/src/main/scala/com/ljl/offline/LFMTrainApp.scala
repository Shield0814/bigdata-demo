package com.ljl.offline


import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{Dataset, SparkSession}

case class Rating(userId: Int, movieId: Int, rate: Double, timestamp: Long)

object LFMTrainApp {

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
        //评分数据
        val ratingDF = spark.read.option("uri", config("mongo.uri"))
            .option("collection", "Rating")
            .format("com.mongodb.spark.sql")
            .load().as[Rating]

        //准备测试数据和训练数据
        val Array(trainData, testData) = ratingDF.randomSplit(Array(0.8, 0.2))
        //3.模型训练


        computeAndEvaluateLFM(trainData, testData)


        //4.停止sparksession
        spark.stop()
    }


    def computeAndEvaluateLFM(trainData: Dataset[Rating], testData: Dataset[Rating]) = {
        //1.1 模型评估器
        val evaluator = new RegressionEvaluator()
            .setMetricName("rmse")
            .setLabelCol("rate")
            .setPredictionCol("prediction")
        //1.2 创建ALS模板
        val als = new ALS()
            .setMaxIter(10)
            .setUserCol("userId")
            .setItemCol("movieId")
            .setRatingCol("rate")

        //1.3 循环训练LFM，找出最佳ALS参数
        val result = for (rank <- Array(20, 40, 50, 80, 100); regParam <- Array(0.001, 0.01, 0.1, 1.0))
            yield {
                als.setRegParam(regParam)
                    .setRank(rank)
                //训练模型
                val LFM = als.fit(trainData)
                val predictions = LFM.transform(testData)
                //评估模型
                val rmse = evaluator.evaluate(predictions)
                (rmse, rank, regParam)
            }

        val tuple = result.minBy(_._1)

        println(tuple)
        tuple
    }


}
