package com.ljl.flink.batch.dataset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

object DatasetSinkAppScala {

    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        //        writeAsTextSink(env)
        writeAsCsvSink(env)
    }


    /**
      * 测试写出到文本
      * 如果并行度为1，则写到文件，如果并行度大于1则输出到目录
      *
      * @param env
      */
    def writeAsTextSink(env: ExecutionEnvironment): Unit = {
        val data = env.fromCollection(1 to 10)

        //        data.writeAsText("file:///d:/data/out/4")
        data.writeAsText("file:///d:/data/out/4", WriteMode.OVERWRITE)
        env.execute("writeAsText")
    }

    def writeAsCsvSink(env: ExecutionEnvironment): Unit = {
        val data = env.fromCollection(Array(
            (1, 2, 3),
            (1, 2, 3),
            (1, 2, 3),
            (1, 2, 3)
        ))

        data.writeAsCsv("file:///d:/data/out/4.csv")
        env.execute("writeAsCsv")
    }
}
