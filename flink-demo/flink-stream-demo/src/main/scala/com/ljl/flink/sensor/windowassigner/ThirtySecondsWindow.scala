package com.ljl.flink.sensor.windowassigner

import java.util
import java.util.Collections

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class ThirtySecondsWindow extends WindowAssigner[Object, TimeWindow] {

    //窗口大小为30s
    val windowSize = 30000L

    override def assignWindows(element: Object,
                               timestamp: Long,
                               context: WindowAssigner.WindowAssignerContext): util.Collection[TimeWindow] = {
        val start = timestamp - timestamp % windowSize
        Collections.singleton(new TimeWindow(start, start + windowSize))
    }

    override def getDefaultTrigger(env: StreamExecutionEnvironment): Trigger[Object, TimeWindow] = {
        EventTimeTrigger.create()
    }

    override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = {
        new TimeWindow.Serializer
    }

    override def isEventTime: Boolean = true
}
