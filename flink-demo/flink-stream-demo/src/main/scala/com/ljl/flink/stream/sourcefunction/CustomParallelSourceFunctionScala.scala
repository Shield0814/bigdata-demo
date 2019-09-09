package com.ljl.flink.stream.sourcefunction

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

class CustomParallelSourceFunctionScala extends ParallelSourceFunction[Int] {
    var count = 1

    @volatile var isRunning = true


    override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        while (isRunning && count < 100000) {
            ctx.collect(count)
            count += 1
            TimeUnit.MILLISECONDS.sleep(100L)
        }
    }

    override def cancel(): Unit = isRunning = false

}
