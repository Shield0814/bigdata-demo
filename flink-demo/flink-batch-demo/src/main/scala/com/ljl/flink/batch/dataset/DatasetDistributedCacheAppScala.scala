package com.ljl.flink.batch.dataset

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.FileUtils

/**
  * 分布式缓存，类似于spark的广播变量，mr的cachefiles,主要用于mapjoin
  */
object DatasetDistributedCacheAppScala {

    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        //注册分布式缓存
        env.registerCachedFile("file:///d:/data/dept.csv", "deptCache")

        val emp = env.readCsvFile[Emp]("file:///d:/data/emp.csv", ignoreFirstLine = true, lenient = true).setParallelism(1)

        emp.map(new RichMapFunction[Emp, (Int, String, Int, String)] {
            private var deptCache = Map[Int, String]()

            override def open(parameters: Configuration): Unit = {
                val deptFile = getRuntimeContext.getDistributedCache.getFile("deptCache")
                val content = FileUtils.readFile(deptFile, "UTF-8")
                val lines = content.split("\n")
                for (line <- lines if !line.startsWith("deptno")) {
                    val fields = line.split(",")
                    deptCache += (fields(0).toInt -> fields(1))
                }
            }

            override def map(value: Emp): (Int, String, Int, String) = {
                if (deptCache.contains(value.deptno)) {
                    (value.empno, value.ename, value.deptno, deptCache(value.deptno))
                } else {
                    (value.empno, value.ename, value.deptno, null)
                }
            }
        }).print()

        //        env.execute("distributed cache ")
    }
}

case class Emp(empno: Int, ename: String, job: String, mgr: Int, hiredate: String, sal: Float, comm: Float, deptno: Int)
