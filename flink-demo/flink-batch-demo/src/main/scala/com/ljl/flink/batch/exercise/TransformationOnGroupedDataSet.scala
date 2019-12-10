package com.ljl.flink.batch.exercise

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector


object TransformationOnGroupedDataSet {

    val data = Array(
        "hadoop,hdfs,mapreduce,yarn",
        "hdfs,namenode,datanode,secondarynamenode",
        "mapreduce,mapTask,ReduceTask,mapjoin,reducejoin,inputformat",
        "mapreduce,mapper,recordReader,map,outputcollector,loopbuffer,kvmeta,kvindex,bufindex,100,80%",
        "mapreduce,mapper,spill,quicksort,merge,mergesort,combiner,compress,snappy,bzip2,gzip,lzo,keygroupingcomparatro",
        "mapreduce,mapper,shuffle,copy,pull,merge,mergesort",
        "mapreduce,reducer,groupcomparator,reduce,outputformat",
        "yarn,resourcemanager,nodemanager,applicationMaster,container",
        "yarn,scheduler,capacity,fifo,fair,preemption,queue",
        "yarn,scheduler,capacity,queue,fifo,acl,max-allocation-memory-mb,min-allocation-memory-mb,rack-locality-delay,node-locality-delay",
        "yarn,scheduler,capacity,queue,fifo,acl,max-allocation-cpu-cores,min-allocation-cup-cores,max-capacity",
        "yarn,scheduler,fair,share,steady share,intanous share,time dimension,preemption,max-resource,state",
        "yarn,scheduler,fair,time fair,loss share",
        "yarn,nodemanager,memory-mb,cup-cores"
    )

    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        val path = "d:\\data\\emp.csv"
        val emps = env.readCsvFile[Emp](path).map(emp => (emp.deptno, emp.sal + emp.comm))

        val groupedDataset = emps.groupBy(0)

        // println("=" * 30)
        // reduceOnGroupedDataSet(groupedDataset).print()
        // println("=" * 30)
        // reduceOnGroupedDataSet(groupedDataset).print()


        //分区内排序
        //        val sortedGroupDataSet = groupedDataset.sortGroup(1, Order.ASCENDING)

        //        reduceGroupOnSortedGroupDataSet(sortedGroupDataSet).print()

        //        combineGroupOnGroupDataSet(groupedDataset).print()
        reduceOnFullDataSet(env)
    }

    def combineOnFullDataSet(env: ExecutionEnvironment) = {
        val data = env.fromCollection(1 to 10)
        data
    }

    def reduceOnFullDataSet(env: ExecutionEnvironment) = {
        val data = env.fromCollection(1 to 10)
        data.reduce(_ + _).print()
    }


    //GroupCombineFunction,贪心策略，
    def combineGroupOnGroupDataSet(groupedDataSet: GroupedDataSet[(String, Float)]) = {
        groupedDataSet.combineGroup((empSals, out: Collector[Float]) => {
            val deptTotalSal = empSals.foldLeft(0.0F)(_ + _._2)
            out.collect(deptTotalSal)

        })
    }


    def aggregateOnGroupedDataSet(groupedDataSet: GroupedDataSet[(String, Float)]) = {
        groupedDataSet.aggregate(Aggregations.MAX, 1)
    }

    def reduceGroupOnSortedGroupDataSet(sortedGroupDataSet: GroupedDataSet[(String, Float)]) = {
        sortedGroupDataSet.reduceGroup(iter => iter.foldLeft(0.0F)(_ + _._2))
    }

    //GroupReduceFunction
    def reduceGroupOnGroupedDataSet(groupedDataSet: GroupedDataSet[(String, Float)]) = {
        groupedDataSet.reduceGroup(iter => iter.foldLeft(0.0F)(_ + _._2))
    }

    def reduceOnGroupedDataSet(groupedDataset: GroupedDataSet[(String, Float)]) = {
        groupedDataset.reduce((ds1, ds2) => (ds1._1, ds1._2 + ds2._2))
    }
}
