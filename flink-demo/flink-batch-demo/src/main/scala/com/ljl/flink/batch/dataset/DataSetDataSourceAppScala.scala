package com.ljl.flink.batch.dataset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

object DataSetDataSourceAppScala {


    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        //从集合创建数据源
        val dataSource1 = env.fromCollection(Array(
            "东方未明,男,28",
            "古实,男,26",
            "沈湘云,女,24"
        ))
        dataSource1.print()
        println("=" * 40)

        //从文本文件创建DataSource
        val dataSource2 = env.readTextFile("file:///d:/data/emp.txt", "UTF-8")
        dataSource2.print()

        println("=" * 40)

        //读取csv文件作为数据源
        val dataSource3 = env.readCsvFile[(Int, Int, Int)]("file:///d:/data/salgrade.csv", ignoreFirstLine = false)
        val dataSource4 = env.readCsvFile[(String, Int)]("file:///d:/data/salgrade.csv", ignoreFirstLine = false, includedFields = Array(0, 2))
        dataSource3.setParallelism(1)
        dataSource4.setParallelism(1)
        dataSource3.print()
        dataSource4.print()

        println("=" * 40)

        //递归读取文本文件
        var conf: Configuration = new Configuration
        conf.setBoolean("recursive.file.enumeration", true)
        val recursiveDataSource = env.readTextFile("file:///d:/data").withParameters(conf)
        println(recursiveDataSource.getParallelism)
    }
}
