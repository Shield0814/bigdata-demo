package com.sitech.hotitems.pf;

import com.sitech.hotitems.entity.CategoryItemStat;
import com.sitech.hotitems.entity.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CategoryItemPVAggregateFunction implements AggregateFunction<UserBehavior, CategoryItemStat, CategoryItemStat> {
    @Override
    public CategoryItemStat createAccumulator() {
        return new CategoryItemStat();
    }

    @Override
    public CategoryItemStat add(UserBehavior ub, CategoryItemStat acc) {
        acc.setCategoryId(ub.getCategoryId());
        acc.setItemId(ub.getItemId());
        acc.setCount(acc.getCount() + 1);
        return acc;
    }

    @Override
    public CategoryItemStat getResult(CategoryItemStat accumulator) {
        return accumulator;
    }

    @Override
    public CategoryItemStat merge(CategoryItemStat a, CategoryItemStat b) {
        a.setCount(a.getCount() + b.getCount());
        return a;
    }
}
