package com.ljl.flink.stream.transformations

import com.ljl.flink.stream.sourcefunction.CustomNonParallelSourceFunctionScala
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TransformationAppScala {

    def main(args: Array[String]): Unit = {

        val senv = StreamExecutionEnvironment.getExecutionEnvironment

        senv.addSource[Int](new CustomNonParallelSourceFunctionScala)
            .filter(_ % 2 == 0)
            .map(x => s"[value = $x]")
            .print()

        senv.execute("filter map transformation")
    }

}
