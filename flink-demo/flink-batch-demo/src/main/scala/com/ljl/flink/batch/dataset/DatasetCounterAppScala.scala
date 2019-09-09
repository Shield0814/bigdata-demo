package com.ljl.flink.batch.dataset

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * flink累加器，计数器案例
  */
object DatasetCounterAppScala {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment

        env.fromCollection(List("spark", "flink", "storm", "hadoop", "kafka"))
            .map(new RichMapFunction[String, String] {
                val counter = new IntCounter(0)

                override def open(parameters: Configuration): Unit = {
                    getRuntimeContext.addAccumulator("element-accumulator", counter)
                }

                override def map(value: String): String = {
                    counter.add(1)
                    value
                }
            }).setParallelism(5).writeAsText("file:///d:/data/out/5", WriteMode.OVERWRITE)

        val result = env.execute("DatasetCounterAppScala")
        val count = result.getAccumulatorResult[Int]("element-accumulator")
        println(s"count = $count")
    }
}
