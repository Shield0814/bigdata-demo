package com.ljl.flink.sensor.processfunction.windowfunction.foldfunction

import com.ljl.flink.sensor.source.SensorReading
import org.apache.flink.api.common.functions.FoldFunction

class LowTempFoldFunction extends FoldFunction[SensorReading, (String, Double)] {

    override def fold(acc: (String, Double), value: SensorReading): (String, Double) = {
        acc match {
            case ("", 100.0) => (value.id, value.temperature)
            case (id, temp) => if (value.temperature < temp) (id, value.temperature) else acc
        }

    }
}
