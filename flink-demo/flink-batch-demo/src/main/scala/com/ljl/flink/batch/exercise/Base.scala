package com.ljl.flink.batch.exercise

case class Emp(empno: String, ename: String, job: String, mgr: String, hiredate: String, sal: Float, comm: Float, deptno: String)

case class Dept(deptno: String, dname: String, loc: String)