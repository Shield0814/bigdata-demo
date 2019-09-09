package com.ljl.flink.batch.dataset

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConversions._

object DatasetBroadcastVariableAppScala {

    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val toBroadcast = env.fromCollection(Array(
            (1, "female"),
            (0, "male")
        ))

        env.fromCollection(Array(
            (1, "aaa", 0),
            (2, "bbb", 1),
            (3, "ccc", 0),
            (4, "ddd", 0),
            (5, "eee", 1),
            (6, "fff", 1),
            (7, "hhh", 0),
            (8, "iii", 1)
        )).map(new RichMapFunction[(Int, String, Int), (Int, String, String)] {
            var genderBroadcast: util.List[(Int, String)] = _

            override def open(parameters: Configuration): Unit = {
                genderBroadcast = getRuntimeContext.getBroadcastVariable[(Int, String)]("genderBroadcast")
            }

            override def map(value: (Int, String, Int)): (Int, String, String) = {
                var gender: String = null
                for ((id, label) <- genderBroadcast if id == value._3) {
                    gender = label
                }
                if (gender != null) {
                    (value._1, value._2, gender)
                } else {
                    (value._1, value._2, null)
                }

            }
        }).withBroadcastSet(toBroadcast, "genderBroadcast")
            .print()
    }
}
