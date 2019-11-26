package com.sitech.hotitems.pf;

import com.sitech.hotitems.entity.CategoryItemStat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TopNKeyedProcessFunction extends KeyedProcessFunction<Tuple, CategoryItemStat, String> {

    private int topSize = 3;

    private ListState<CategoryItemStat> topNCategoryItemState = null;

    public TopNKeyedProcessFunction(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        topNCategoryItemState = getRuntimeContext().getListState(
                new ListStateDescriptor<CategoryItemStat>("topNCategoryItemState", CategoryItemStat.class)
        );
    }

    @Override
    public void processElement(CategoryItemStat stat,
                               Context ctx,
                               Collector<String> out) throws Exception {
        topNCategoryItemState.add(stat);
        ctx.timerService().registerEventTimeTimer(stat.getWindowEndTs() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        List<CategoryItemStat> allItemStats = new ArrayList<>();
        Iterator<CategoryItemStat> iter = topNCategoryItemState.get().iterator();
        while (iter.hasNext()) {
            allItemStats.add(iter.next());
        }
        topNCategoryItemState.clear();

        Collections.sort(allItemStats, (o1, o2) -> Long.compare(o2.getCount(), o1.getCount()));
        //将topN信息格式化输出
        StringBuilder sb = new StringBuilder("==================================\n");
        for (int i = 0; i < topSize && i < allItemStats.size(); i++) {
            sb.append("品类id: ").append(allItemStats.get(i).getCategoryId()).append("\n")
                    .append("排名: ").append(i)
                    .append("  商品ID=").append(allItemStats.get(i).getItemId())
                    .append("  浏览量=").append(allItemStats.get(i).getCount()).append("\n");
        }
        sb.append("====================================\n\n");

        Thread.sleep(1000);
        out.collect(sb.toString());
    }
}
