package com.ljl.flink.batch.exercise

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

/**
  * 注意join hint的6中算法
  * OPTIMIZER_CHOOSES：由系统决定
  * BROADCAST_HASH_FIRST：A good strategy if the first input is very small.
  * BROADCAST_HASH_SECOND：A good strategy if the second input is very small.
  * REPARTITION_HASH_FIRST：This strategy is good if the first input is smaller than the second, but both inputs are still large
  * REPARTITION_HASH_SECOND：This strategy is good if the second input is smaller than the first, but both inputs are still large.
  * REPARTITION_SORT_MERGE：This strategy is good if one or both of the inputs are already sorted
  */
object JoinTransformation {


    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(5)
        val tool = ParameterTool.fromArgs(args)
        var empPath = "d:\\data\\emp.csv"
        var deptPath = "d:\\data\\dept.csv"
        if (tool.has("empPath"))
            empPath = tool.get("empPath")
        if (tool.has("deptPath"))
            deptPath = tool.get("deptPath")
        val empDataSet = env.readCsvFile[Emp](empPath)
        val deptDataSet = env.readCsvFile[Dept](deptPath)


    }


    /**
      * 测试joinHint,
      * 注意 join，leftOuterJoin, rightOuterJoin, fullOuterJoin支持的joinHint不同
      *
      * @param empDataSet
      * @param deptDataSet
      */
    def joinWithJoinHint(empDataSet: DataSet[Emp], deptDataSet: DataSet[Dept], tool: ParameterTool) = {
        var empDeptDataSet: JoinDataSet[Emp, Dept] = null
        if (tool.has("strategy")) {
            tool.get("strategy").toUpperCase match {
                case "BROADCAST_HASH_FIRST" =>
                    empDeptDataSet = empDataSet.join(deptDataSet, JoinHint.BROADCAST_HASH_FIRST)
                        .where(_.deptno).equalTo(_.deptno)
                case "BROADCAST_HASH_SECOND" =>
                    empDeptDataSet = empDataSet.join(deptDataSet, JoinHint.BROADCAST_HASH_SECOND)
                        .where(_.deptno).equalTo(_.deptno)
                case "REPARTITION_HASH_FIRST" =>
                    empDeptDataSet = empDataSet.join(deptDataSet, JoinHint.REPARTITION_HASH_FIRST)
                        .where(_.deptno).equalTo(_.deptno)
                case "REPARTITION_HASH_SECOND" =>
                    empDeptDataSet = empDataSet.join(deptDataSet, JoinHint.REPARTITION_HASH_SECOND)
                        .where(_.deptno).equalTo(_.deptno)
                case "REPARTITION_SORT_MERGE" =>
                    empDeptDataSet = empDataSet.join(deptDataSet, JoinHint.REPARTITION_SORT_MERGE)
                        .where(_.deptno).equalTo(_.deptno)
                case _ =>
                    println("输入错误")
            }
        } else {
            empDeptDataSet = empDataSet.join(deptDataSet, JoinHint.OPTIMIZER_CHOOSES)
                .where(_.deptno).equalTo(_.deptno)
        }

        empDeptDataSet.print()
    }
}
