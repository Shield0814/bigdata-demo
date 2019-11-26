package com.ljl.spark.sql.udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

case class User(id: Int, age: Int)


object MyAvgAggregatorApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MyAvgAggregatorApp").setMaster("local[*]")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        val userRDD = spark.sparkContext.parallelize(Array(
            User(1, 20),
            User(1, 25),
            User(1, 23),
            User(1, 40)
        ))
        import spark.implicits._
        val userDS: Dataset[User] = userRDD.toDS()

        val avg = new MyAvgAggregator

        userDS.select(avg.toColumn).show()


        spark.stop()
    }
}

class MyAvgAggregator extends Aggregator[User, (Double, Long), Double] {
    override def zero: (Double, Long) = (0.0, 0)

    override def reduce(b: (Double, Long), a: User): (Double, Long) = (b._1 + a.age, b._2 + 1)

    override def merge(b1: (Double, Long), b2: (Double, Long)): (Double, Long) =
        (b1._1 + b2._1, b1._2 + b2._2)

    override def finish(reduction: (Double, Long)): Double = reduction._1 / reduction._2

    override def bufferEncoder: Encoder[(Double, Long)] =
        Encoders.tuple(Encoders.scalaDouble, Encoders.scalaLong)

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

