package com.ljl.flink.stream.sourcefunction

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamSourceFunctionAppScala {

    def main(args: Array[String]): Unit = {
        val senv = StreamExecutionEnvironment.getExecutionEnvironment
        //        customNonParallelSourceFunctionTest(senv)
        customRichParallelSourceFunctionTest(senv)
    }

    def customRichParallelSourceFunctionTest(senv: StreamExecutionEnvironment): Unit = {
        val data = senv.addSource[Int](new CustomRichParallelSourceFunctionScala)
            .setParallelism(3)
        data.print()
        senv.execute("customRichParallelSourceFunctionTest")
    }

    def customParallelSourceFunctionTest(senv: StreamExecutionEnvironment): Unit = {
        val data = senv.addSource[Int](new CustomParallelSourceFunctionScala)
            .setParallelism(3)
        data.print()
        senv.execute("customParallelSourceFunctionTest")
    }

    /**
      * 非并行数据源,并行度设置成大于1的话，会报参数不合法异常：
      * Exception in thread "main" java.lang.IllegalArgumentException: Source: 1 is not a parallel source
      *
      * @param senv
      */
    def customNonParallelSourceFunctionTest(senv: StreamExecutionEnvironment): Unit = {
        val data = senv.addSource[Int](new CustomNonParallelSourceFunctionScala)
            .setParallelism(2)
        data.print()
        senv.execute("customNonParallelSourceFunctionTest")
    }
}
