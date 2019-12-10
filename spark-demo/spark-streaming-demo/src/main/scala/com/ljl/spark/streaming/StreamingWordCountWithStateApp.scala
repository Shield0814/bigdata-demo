package com.ljl.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCountWithStateApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("StreamingWordCountWithStateApp")
            .setMaster("local[*]")
        val checkPointDir = "D:\\data\\checkpoint"
        val ssc = StreamingContext.getOrCreate(checkPointDir, () => createContext(conf, checkPointDir))

        ssc.start()
        ssc.awaitTermination()
    }

    def createContext(conf: SparkConf, checkPointDir: String) = {
        val ssc = new StreamingContext(conf, Seconds(5))


        ssc.checkpoint(checkPointDir)

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
        ssc
    }
}
