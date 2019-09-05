package com.ljl.hadoop.mr.reducejoin;

import com.ljl.hadoop.common.key.EmpDeptWritableComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 部门id，即join key，根据join key分组，而不是默认的自定义的key分组
 */
public class EmpDeptGroupingComparator extends WritableComparator {

    protected EmpDeptGroupingComparator() {
        super(EmpDeptWritableComparable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        EmpDeptWritableComparable ao = (EmpDeptWritableComparable) a;
        EmpDeptWritableComparable bo = (EmpDeptWritableComparable) b;
        return Integer.compare(ao.getDeptno(), bo.getDeptno());
    }
}
