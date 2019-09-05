package com.ljl.hadoop.mr.reducejoin;

import com.ljl.hadoop.common.key.EmpDeptWritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 由于通过自定义 GroupingComparator ，reducer端的数据的第一条肯定是该key要join短表的数据
 * 用第一条数据更新该key后面key中要获得的数据
 */
public class EmpDeptReducer extends Reducer<EmpDeptWritableComparable, NullWritable, EmpDeptWritableComparable, NullWritable> {

    private String dname;
    private String loc;

    @Override
    protected void reduce(EmpDeptWritableComparable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iter = values.iterator();
        iter.next();
        dname = key.getDname();
        loc = key.getLoc();
        while (iter.hasNext()) {
            iter.next();
            key.setDname(dname);
            key.setLoc(loc);
            context.write(key, NullWritable.get());
        }

    }
}
