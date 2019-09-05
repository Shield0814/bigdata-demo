package com.ljl.hadoop.mr.reducejoin;

import com.ljl.hadoop.common.key.EmpDeptWritableComparable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * reduce join案例要点：
 * 1. mapper端把需要join的两张表都读进来，并封装到一个key对象中
 * 2. mapper端对需要join的两张表的数据进行打标区分，通过重写setup方法和map实现
 * 3. 自定义 GroupingComparator， 从而在reducer端对数据读取时把join key相同的数据放在一起读取;
 * 使得reducer读取数据时把同一key短表的数据放在第一条
 * 自定义GroupingComparator细节参考com.ljl.hadoop.mr.groupingcomparator.OrderSortComparator
 * 4. reducer端 每个key读取数据获得第一条数据，作为后面key的要更新的值
 */
public class EmpDeptMapper extends Mapper<LongWritable, Text, EmpDeptWritableComparable, NullWritable> {

    private String tableName;

    private EmpDeptWritableComparable empKeyOut = new EmpDeptWritableComparable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        if (split.getPath().getName().contains("emp")) {
            tableName = "emp";
        } else if (split.getPath().getName().contains("dept")) {
            tableName = "dept";
        } else {
            throw new InterruptedException("输入数据有问题");
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //数据格式：7499,ALLEN,SALESMAN,7698,1981/2/20,1600.00,300.00,30
        String[] strs = value.toString().split(",");
        if (tableName.equals("emp")) {
            empKeyOut.setEmpno(Integer.parseInt(strs[0]));
            empKeyOut.setEname(strs[1]);
            empKeyOut.setJob(strs[2]);
            if (!StringUtils.isEmpty(strs[3])) {
                empKeyOut.setMgr(Integer.parseInt(strs[3]));
            }
            empKeyOut.setHireDate(strs[4]);
            if (!StringUtils.isEmpty(strs[5])) {
                empKeyOut.setSal(Float.parseFloat(strs[5]));
            }

            if (!StringUtils.isEmpty(strs[6])) {
                empKeyOut.setComm(Float.parseFloat(strs[6]));
            }
            if (!StringUtils.isEmpty(strs[7])) {
                empKeyOut.setDeptno(Integer.parseInt(strs[7]));
            }
            empKeyOut.setDname("");
            empKeyOut.setLoc("");
        }
        if (tableName.equals("dept")) {
            empKeyOut.setEmpno(0);
            empKeyOut.setEname("");
            empKeyOut.setJob("");
            empKeyOut.setMgr(0);
            empKeyOut.setHireDate("");
            empKeyOut.setSal(0.0F);
            empKeyOut.setComm(0.0F);
            if (!StringUtils.isEmpty(strs[0])) {
                empKeyOut.setDeptno(Integer.parseInt(strs[0]));
            }
            empKeyOut.setDname(strs[1]);
            empKeyOut.setLoc(strs[2]);
        }
        context.write(empKeyOut, NullWritable.get());
    }
}
