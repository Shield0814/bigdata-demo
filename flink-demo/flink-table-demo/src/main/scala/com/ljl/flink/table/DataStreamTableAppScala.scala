package com.ljl.flink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{StreamTableEnvironment, TableEnvironment}

/**
  * 使用table api处理dataStream
  */
object DataStreamTableAppScala {


    def main(args: Array[String]): Unit = {
        //1. 获取dataStream执行环境
        val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2. 通过DataStream执行环境获取Table API 处理DataStream的执行环境
        val tenv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(senv)

        val socketDataStream = senv.socketTextStream("bigdata116", 9999)
        //3. 注册表，类似于spark sql中的createOrReplaceTable()
        //        val tblEmp: Table = new Table()
        //        tenv.registerTable("tbl_emp",tblEmp)

        //4. 注册输出sink
        //        tenv.registerTableSink()

    }
}
