package com.ljl.flink.stream.watermark

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

class CustomWaterMarkAssigner(val maxOutOfOrderness: Time) extends AssignerWithPeriodicWatermarks[SensorReading] {

    var currentMaxTimestamp = Long.MinValue + maxOutOfOrderness.toMilliseconds
    var lastEmittedWatermark = Long.MinValue

    override def getCurrentWatermark: Watermark = {
        val potentialWM = currentMaxTimestamp - maxOutOfOrderness.toMilliseconds;
        if (potentialWM >= lastEmittedWatermark) {
            lastEmittedWatermark = potentialWM;
        }
        new Watermark(lastEmittedWatermark)
    }

    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
        val currentTimestamp = element.timestamp * 1000
        if (currentTimestamp > currentMaxTimestamp) {
            currentMaxTimestamp = currentTimestamp
        }
        currentTimestamp
    }

}
