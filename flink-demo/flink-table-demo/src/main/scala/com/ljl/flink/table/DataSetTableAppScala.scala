package com.ljl.flink.table

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

object DataSetTableAppScala {

    def main(args: Array[String]): Unit = {

        //1. 获取dataset执行环境
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //2. 通过dataset执行环境获得table执行环境
        val tenv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

        val empDataset = env.readCsvFile[Emp]("file:///d:/data/emp.csv", lenient = true, ignoreFirstLine = false)

        //通过dataset创建table
        val empTable = tenv.fromDataSet(empDataset)

        //注册表
        tenv.registerTable("tbl_emp", empTable)

        //执行sql
        val resultTable = tenv.sqlQuery("select deptno,max(sal) as max_sal from tbl_emp group by deptno")

        //输出结果
        tenv.toDataSet[Row](resultTable).print()


    }

    case class Emp(empno: Int, ename: String, job: String, mgr: Int, hireDate: String, sal: Float, comm: Float, deptno: Int)

}
