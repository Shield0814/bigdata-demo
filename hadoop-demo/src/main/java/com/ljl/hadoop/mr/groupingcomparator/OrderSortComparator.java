package com.ljl.hadoop.mr.groupingcomparator;

import com.ljl.hadoop.common.key.OrderKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义订单分组排序器，作用：
 * reducer在拉取mapper端的数据时会根据分组排序器来判断是否时相同key，
 * 相同的key只会进入reducer一次，即使后面还存在该key，他的值也不会收集到reducer的Iterable中
 * 问题点：
 * 必须重写构造方法，并且createInstances必须为true，否则reducer端读取数据时会报空指针异常
 */
public class OrderSortComparator extends WritableComparator {

    public OrderSortComparator() {
        //createInstances 必须设置为true,否则会报空指针异常
        super(OrderKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderKey o1 = (OrderKey) a;
        OrderKey o2 = (OrderKey) b;
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
