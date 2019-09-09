package com.ljl.flink.batch.dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object DatasetTransformationAppScala {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment
        //        mapOperator(env)
        //        flapMapOperator(env)
        //        filterOperator(env)
        //        firstOperator(env)
        //        firstOperator2(env)
        //        mapPartitionOperator(env)
        //        distinctOperator(env)

        //        joinOperatory(env)
        //        leftOuterJoinOperatory(env)
        //        rightOuterJoinOperatory(env)
        crossOperatory(env)
    }

    /**
      * cross 操作测试
      *
      * @param env
      */
    def crossOperatory(env: ExecutionEnvironment): Unit = {
        val emp = env.fromCollection(Array(
            (1, "smith", "10"),
            (2, "mark", "10"),
            (3, "lili", "20"),
            (4, "janet", "30"),
            (5, "john", "40"),
            (6, "沈湘云", "30")
        ))
        val dept = env.fromCollection(Array(
            ("10", "sales"),
            ("20", "dev"),
            ("30", "search"),
            ("50", "ceo")
        ))

        emp.cross(dept)
            .apply((first, second) => {
                if (first == null) {
                    (null, null, null, second._1, second._2)
                } else if (second == null) {
                    (first._1, first._2, first._3, null, null)
                } else {
                    (first._1, first._2, first._3, second._1, second._2)
                }
            }).print()

    }

    /**
      * rightOuterJoin 操作测试
      *
      * @param env
      */
    def rightOuterJoinOperatory(env: ExecutionEnvironment): Unit = {
        val emp = env.fromCollection(Array(
            (1, "smith", "10"),
            (2, "mark", "10"),
            (3, "lili", "20"),
            (4, "janet", "30"),
            (5, "john", "40"),
            (6, "沈湘云", "30")
        ))
        val dept = env.fromCollection(Array(
            ("10", "sales"),
            ("20", "dev"),
            ("30", "search"),
            ("50", "ceo")
        ))

        emp.rightOuterJoin(dept)
            .where(2)
            .equalTo(0)
            .apply((first, second) => {
                if (first == null) {
                    (null, null, null, second._1, second._2)
                } else {
                    (first._1, first._2, first._3, second._1, second._2)
                }

            }).print()

    }


    /**
      * leftOuterJoin 操作测试
      *
      * @param env
      */
    def leftOuterJoinOperatory(env: ExecutionEnvironment): Unit = {
        val emp = env.fromCollection(Array(
            (1, "smith", "10"),
            (2, "mark", "10"),
            (3, "lili", "20"),
            (4, "janet", "30"),
            (5, "john", "40"),
            (6, "沈湘云", "30")
        ))
        val dept = env.fromCollection(Array(
            ("10", "sales"),
            ("20", "dev"),
            ("30", "search"),
            ("50", "ceo")
        ))

        emp.leftOuterJoin(dept)
            .where(2)
            .equalTo(0)
            .apply((first, second) => {
                //左连接时，如果左表存在右表不存在的数据，则second的为Null需要处理，否则会报空指针异常
                if (second != null) {
                    (first._1, first._2, first._3, second._2)
                } else {
                    (first._1, first._2, first._3, "")
                }

            }).print()

    }

    /**
      * join 操作测试
      *
      * @param env
      */
    def joinOperatory(env: ExecutionEnvironment): Unit = {
        val emp = env.fromCollection(Array(
            (1, "smith", "10"),
            (2, "mark", "10"),
            (3, "lili", "20"),
            (4, "janet", "30"),
            (5, "john", "40"),
            (6, "沈湘云", "30")
        ))
        val dept = env.fromCollection(Array(
            ("10", "sales"),
            ("20", "dev"),
            ("30", "search")
        ))

        emp.join(dept)
            .where(2)
            .equalTo(0)
            .apply((first, second) => {
                (first._1, first._2, first._3, second._2)
            }).print()

    }

    /**
      * first 操作测试
      *
      * @param env
      */
    def firstOperator2(env: ExecutionEnvironment): Unit = {
        val data = env.fromCollection(Array(
            (1, "Hadoop"),
            (3, "Flink"),
            (1, "Spark"),
            (1, "Storm"),
            (2, "RDD"),
            (2, "DATASET"),
            (2, "DataSteam"),
            (3, "DataFrame"),
            (3, "DStream"),
            (3, "DataStream")
        ))
        data.groupBy(0)
            .sortGroup(1, Order.ASCENDING)
            .first(2).print()
    }

    /**
      * distinct 操作测试
      *
      * @param env
      */
    def distinctOperator(env: ExecutionEnvironment): Unit = {
        val data = env.fromCollection(Array(1, 1, 2, 1, 2, 3, 1, 1, 1, 3, 1, 3, 1, 3))
        data.distinct().print()
    }

    /**
      * mapPartition 操作测试
      *
      * @param env
      */
    def mapPartitionOperator(env: ExecutionEnvironment): Unit = {

        val data = env.fromCollection(Array(
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
        )).setParallelism(4).mapPartition(iter => {
            println("+" * 100)
            iter.map(item => ">>" + item)
        }).print()


    }

    /**
      * first 操作测试
      *
      * @param env
      */
    def firstOperator(env: ExecutionEnvironment): Unit = {
        val data = env.fromCollection(Array(
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
        ))
        data.first(2).setParallelism(1).print()
    }

    /**
      * filter 操作测试
      *
      * @param env
      */
    def filterOperator(env: ExecutionEnvironment): Unit = {
        val data = env.fromCollection(Array(
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
        ))
        val res = data.flatMap(_.split(",")).filter(s => s.length > 10).setParallelism(1)
        res.print()
    }

    /**
      * flatMap 操作测试
      *
      * @param env flink 批处理运行环境
      */
    def flapMapOperator(env: ExecutionEnvironment): Unit = {
        val data = env.fromCollection(Array(
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
        ))
        data.flatMap(s => s.split(",")).setParallelism(1).print()
    }

    /**
      * 测试flink map操作
      *
      * @param env flink 批处理运行环境
      */
    def mapOperator(env: ExecutionEnvironment) = {
        val data = env.fromCollection(1 to 10)
        data.map(v => Math.random() + v).setParallelism(1)
        data.print()
    }


}
