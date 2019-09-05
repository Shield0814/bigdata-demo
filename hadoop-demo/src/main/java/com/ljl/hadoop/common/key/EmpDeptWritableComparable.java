package com.ljl.hadoop.common.key;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EmpDeptWritableComparable implements WritableComparable<EmpDeptWritableComparable> {

    //员工编号
    private int empno;

    private String ename;
    private String job;
    private int mgr;
    private String hireDate;
    private float sal;
    private float comm;
    private int deptno;
    private String dname;
    private String loc;


    public EmpDeptWritableComparable() {
    }

    public int getEmpno() {
        return empno;
    }

    public void setEmpno(int empno) {
        this.empno = empno;
    }

    public String getEname() {
        return ename;
    }

    public void setEname(String ename) {
        this.ename = ename;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public int getMgr() {
        return mgr;
    }

    public void setMgr(int mgr) {
        this.mgr = mgr;
    }

    public String getHireDate() {
        return hireDate;
    }

    public void setHireDate(String hireDate) {
        this.hireDate = hireDate;
    }

    public float getSal() {
        return sal;
    }

    public void setSal(float sal) {
        this.sal = sal;
    }

    public float getComm() {
        return comm;
    }

    public void setComm(float comm) {
        this.comm = comm;
    }

    public int getDeptno() {
        return deptno;
    }

    public void setDeptno(int deptno) {
        this.deptno = deptno;
    }

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

    @Override
    public int compareTo(EmpDeptWritableComparable o) {
        if (Integer.compare(this.getDeptno(), o.getDeptno()) == 0) {
            return o.getDname().compareTo(this.getDname());
        } else {
            return Integer.compare(this.getDeptno(), o.getDeptno());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(empno);
        out.writeUTF(ename);
        out.writeUTF(job);
        out.writeInt(mgr);
        out.writeUTF(hireDate);
        out.writeFloat(sal);
        out.writeFloat(comm);
        out.writeInt(deptno);
        out.writeUTF(dname);
        out.writeUTF(loc);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.empno = in.readInt();
        this.ename = in.readUTF();
        this.job = in.readUTF();
        this.mgr = in.readInt();
        this.hireDate = in.readUTF();
        this.sal = in.readFloat();
        this.comm = in.readFloat();
        this.deptno = in.readInt();
        this.dname = in.readUTF();
        this.loc = in.readUTF();
    }

    @Override
    public String toString() {
        return empno +
                "," + ename +
                "," + job +
                "," + mgr +
                "," + hireDate +
                "," + sal +
                "," + comm +
                "," + deptno +
                "," + dname +
                "," + loc;
    }
}
