package com.ljl.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCountWithStateApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("StreamingWordCountWithStateApp")
            .setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(5))

        ssc.checkpoint("./checkpoint")

        val lineDStream = ssc.socketTextStream("bigdata116", 9999)

        def updateFunc(counts: Seq[Int], state: Option[Long]) = {
            state match {
                case Some(stateValue) =>
                    Some(stateValue + counts.sum)
                case None =>
                    Some(counts.sum.toLong)
            }
        }

        lineDStream.flatMap(_.split("\\W"))
            .map((_, 1))
            .updateStateByKey[Long](updateFunc _)
            .print()

        ssc.start()
        ssc.awaitTermination()
    }
}
