package com.ljl.flink.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * 使用table api处理dataStream
  */
object DataStreamTableAppScala {

    case class Emp(empno: Int, ename: String, job: String, mgr: Int, hireDate: String, sal: Float, comm: Float, deptno: Int)

    def main(args: Array[String]): Unit = {
        //1. 获取dataStream执行环境
        val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //2. 通过DataStream执行环境获取Table API 处理DataStream的执行环境
        val tenv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(senv)

        val empDataStream = senv.socketTextStream("bigdata116", 6666)
            .map(line => {
                val tmp = line.split(",")
                Emp(tmp(0).toInt, tmp(1), tmp(2), tmp(3).toInt, tmp(4), tmp(5).toFloat, tmp(6).toFloat, tmp(6).toInt)
            })

        //3. 注册表，类似于spark sql中的createOrReplaceTable()


        //4. 注册输出sink
        //        tenv.registerTableSink()

    }
}
