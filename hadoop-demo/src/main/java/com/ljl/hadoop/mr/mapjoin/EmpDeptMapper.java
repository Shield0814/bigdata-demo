package com.ljl.hadoop.mr.mapjoin;

import com.ljl.hadoop.common.key.EmpDeptWritableComparable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * mapjoin 案例
 * 要点：
 * 1. mapper启动时，从分布式缓存中加载小表
 * 2. reducer数量设置成0，即，不启动reducer
 * 3. driver端设置缓存文件 cacheFiles
 */
public class EmpDeptMapper extends Mapper<LongWritable, Text, EmpDeptWritableComparable, NullWritable> {

    private Map<String, Dept> deptCache = new HashMap<>();
    private EmpDeptWritableComparable empKeyOut = new EmpDeptWritableComparable();

    class Dept {
        String dname;
        String loc;

        public String getDname() {
            return dname;
        }

        public void setDname(String dname) {
            this.dname = dname;
        }

        public String getLoc() {
            return loc;
        }

        public void setLoc(String loc) {
            this.loc = loc;
        }

        public Dept(String dname, String loc) {
            this.dname = dname;
            this.loc = loc;
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles.length != 0) {
            for (URI uri : cacheFiles) {
                FSDataInputStream in = fs.open(new Path(uri.getPath()), 1024);
                InputStreamReader reader = new InputStreamReader(in, "UTF-8");
                BufferedReader br = new BufferedReader(reader, 1024);
                String line = br.readLine();
                while (!StringUtils.isEmpty(line)) {
                    String[] fields = line.split(",");
                    deptCache.put(fields[0], new Dept(fields[1], fields[2]));
                    line = br.readLine();
                }
                br.close();
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //数据格式：7499,ALLEN,SALESMAN,7698,1981/2/20,1600.00,300.00,30
        String[] strs = value.toString().split(",");

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
        Dept dept = deptCache.get(String.valueOf(empKeyOut.getDeptno()));
        empKeyOut.setDname(dept.getDname());
        empKeyOut.setLoc(dept.getLoc());
        context.write(empKeyOut, NullWritable.get());
    }
}
