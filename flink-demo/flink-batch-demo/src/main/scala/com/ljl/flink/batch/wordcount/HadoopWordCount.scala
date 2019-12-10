package com.ljl.flink.batch.wordcount


import java.lang

import org.apache.flink.api.common.functions.{GroupReduceFunction, ReduceFunction}
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

class SumReduceFunction extends ReduceFunction[(String, Int)] {
    override def reduce(value1: (String, Int),
                        value2: (String, Int)): (String, Int) = {
        (value1._1, value1._2 + value2._2)
    }
}

class SumGroupReduceFunction extends GroupReduceFunction[(String, Int), String] {
    override def reduce(values: lang.Iterable[(String, Int)],
                        out: Collector[String]): Unit = {
        val tuple = (("", 0) /: values) ((state, wc) => (wc._1, state._2 + wc._2))
        out.collect(s"${tuple._1} -> ${tuple._2}")
    }
}

object HadoopWordCount {

    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        val inputF = new TextInputFormat(new Path("file:///d:/data/app.log"))
        val hadoopResult = env.createInput(inputF)
            .flatMap(_.split("\\W"))
            .filter(!_.matches("\\d+"))
            .map((_, 1))
            .groupBy(0)
            // .sum(1)
            // .reduce(new SumReduceFunction)
            // .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
            // .reduce(new SumReduceFunction,CombineHint.HASH)
            // .reduceGroup(iter => iter.foldLeft(("", 0))((state, wc) => (wc._1, wc._2 + state._2)))
            // .reduceGroup(iter => (("", 0) /: iter) ((state, wc) => (wc._1, wc._2 + state._2)))
            .reduceGroup(new SumGroupReduceFunction)
        hadoopResult.print()
        // hadoopResult.writeAsText("file:///d:/data/out", WriteMode.OVERWRITE).setParallelism(2)


        //        env.execute("HadoopWordCount")

    }

}
