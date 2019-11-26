package com.sitech.hotitems.pf;

import com.sitech.hotitems.entity.CategoryItemStat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 取出window中每个key（categoryId_item）
 */
public class WindowResultFunction implements WindowFunction<CategoryItemStat, CategoryItemStat, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple,
                      TimeWindow window,
                      Iterable<CategoryItemStat> input,
                      Collector<CategoryItemStat> out) throws Exception {

        Tuple2<Integer, Long> key = (Tuple2<Integer, Long>) tuple;
        Integer categoryId = key.f0;
        Long itemId = key.f1;
        CategoryItemStat stat = input.iterator().next();
        stat.setWindowEndTs(window.getEnd());
        out.collect(stat);
    }
}
