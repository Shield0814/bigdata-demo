package com.ljl.spark.sql.udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

case class SalGrade(id: Int, low: Double, high: Double)

object MyAvgUDAFApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MyAvgUDAFApp").setMaster("local[*]")
        val spark = SparkSession.builder()
            .config(conf)
            //            .enableHiveSupport()
            .getOrCreate()
        val sc = spark.sparkContext
        import spark.implicits._
        val path = "D:\\data\\salgrade.csv"
        val salGradeDF = spark.read.csv(path).toDF("id", "low", "high")
        salGradeDF.printSchema()

        spark.udf.register("myavg", new MyAvgUDAF)

        //        salGradeDF.createOrReplaceTempView("salgrade")
        //        spark.sql("select myavg(low) as myavg from salgrade").show()


        salGradeDF.agg(
            avg($"low").as("avg")
        ).show()

        spark.stop()


    }
}


class MyAvgUDAF extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = new StructType(Array(StructField("input", DoubleType)))

    override def bufferSchema: StructType = new StructType(Array(
        StructField("sum", DoubleType),
        StructField("count", LongType)
    ))

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0.0
        buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getDouble(0) + input.getDouble(0)
        buffer(1) = buffer.getLong(1) + 1L
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

    }

    override def evaluate(buffer: Row): Double = buffer.getDouble(0) / buffer.getLong(1)
}
