package com.ljl.flink.batch.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object WordCountScala {

    def main(args: Array[String]): Unit = {

        //1. 从命令行参数中解析我们应用需要的参数
        val tool = ParameterTool.fromArgs(args)

        //2. 获得运行时环境
        val env = ExecutionEnvironment.getExecutionEnvironment

        var lines: DataSet[String] = null
        //3. 读取文件
        if (tool.has("input")) {
            lines = env.readTextFile(tool.get("input"))
        } else {
            lines = env.fromCollection(Array("hello! welcome to bejing",
                "hello! welcome to flink's world",
                "hello! welcome to flink datastream world",
                "hello! welcome to bejing",
                "hello! welcome to bejing"
            ))
        }
        //4. transform，计算词频
        val wordCount = lines.flatMap(_.split("\\W"))
            .map((_, 1))
            .groupBy(0)
            .sum(1)
        //5. action，输出结果
        if (tool.has("output")) {
            wordCount.setParallelism(1).writeAsText(tool.get("output"))
        } else {
            wordCount.print()
        }
    }

}
