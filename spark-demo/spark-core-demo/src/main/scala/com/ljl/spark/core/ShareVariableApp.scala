package com.ljl.spark.core

import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer
import java.util.{Map => JMap}

import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计每个部门的平均工资，最高工资，最低工资，总工资，总人数,并显示部门名称
  */

//员工类
case class Emp(empno: Int,
               ename: String,
               job: String,
               mgr: Int,
               hiredate: String,
               sal: Double,
               comm: Double,
               deptno: Int)


// 部门类
case class Dept(deptno: Int, dname: String, loc: String)

case class DeptStat(deptno: Int, dname: String, maxSal: Double, minSal: Double, totalSal: Double, totalCount: Int)

//输入数据：部门信息，员工信息
//输出信息：(部门编号，部门名称，平均工资，最高工资，最低工资，总工资，总人数)的map
class DeptInfoAccumator extends AccumulatorV2[(Emp, Dept), JMap[Int, DeptStat]] {

    lazy val deptAcc = new ConcurrentHashMap[Int, DeptStat]()
    lazy val deptAcc2 = new Int2ObjectRBTreeMap[DeptStat]()


    override def isZero: Boolean = deptAcc.isEmpty

    override def copy(): AccumulatorV2[(Emp, Dept), JMap[Int, DeptStat]] = {
        val newAcc = new DeptInfoAccumator
        synchronized {
            deptAcc.entrySet().forEach(new Consumer[JMap.Entry[Int, DeptStat]] {
                override def accept(t: JMap.Entry[Int, DeptStat]): Unit = {
                    newAcc.deptAcc.put(t.getKey, t.getValue)
                }
            })
        }
        newAcc
    }

    override def reset(): Unit = {
        deptAcc.clear()
    }

    override def add(v: (Emp, Dept)): Unit = {
        val deptno = v._2.deptno
        val dname = v._2.dname
        val sal = v._1.sal + v._1.comm
        if (deptAcc.containsKey(deptno)) {
            val deptAccTmp = deptAcc.get(deptno)
            deptAcc.put(deptno, DeptStat(deptno,
                dname,
                sal.max(deptAccTmp.maxSal),
                sal.min(deptAccTmp.minSal),
                sal + deptAccTmp.totalSal,
                1 + deptAccTmp.totalCount))
        } else {
            deptAcc.put(v._2.deptno, DeptStat(deptno, dname, sal, sal, sal, 1))
        }
    }

    override def merge(other: AccumulatorV2[(Emp, Dept), JMap[Int, DeptStat]]): Unit = {
        val oAcc = other.asInstanceOf[DeptInfoAccumator]

        oAcc.deptAcc.entrySet().forEach(new Consumer[JMap.Entry[Int, DeptStat]] {
            override def accept(entry: JMap.Entry[Int, DeptStat]): Unit = {
                val k = entry.getKey
                val v = entry.getValue
                val deptno = k
                val dname = v.dname
                val maxSal = v.maxSal
                val minSal = v.minSal
                val count = v.totalCount
                val totoalSal = v.totalSal
                if (deptAcc.containsKey(deptno)) {
                    val deptAccTmp = deptAcc.get(deptno)
                    deptAcc.put(deptno, DeptStat(deptno,
                        dname,
                        maxSal.max(deptAccTmp.maxSal),
                        minSal.min(deptAccTmp.minSal),
                        totoalSal + deptAccTmp.totalSal,
                        count + deptAccTmp.totalCount))
                } else {
                    deptAcc.put(deptno, DeptStat(deptno, dname, maxSal, minSal, totoalSal, count))
                }
            }
        })

    }

    override def value: JMap[Int, DeptStat] = deptAcc

}

object ShareVariableApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("share variable App")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(
                classOf[DeptStat],
                classOf[Emp],
                classOf[Dept],
                classOf[ConcurrentHashMap[Int, DeptStat]],
                classOf[collection.immutable.Map[Int, Dept]]
            ))
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

        //员工信息
        val empInfo = sc.textFile("D:\\data\\emp.csv")
            .map(line => {
                val fields = line.split(",")
                Emp(fields(0).toInt, fields(1), fields(2),
                    fields(3).toInt, fields(4), fields(5).toDouble,
                    fields(6).toDouble, fields(7).toInt)
            })


        //部门信息
        val deptInfo = sc.textFile("D:\\data\\dept.csv")
            .map(line => {
                val fields = line.split(",")
                (fields(0).toInt, Dept(fields(0).toInt, fields(1), fields(2)))
            }).collect().toMap

        //广播部门信息
        val bcDeptInfo = sc.broadcast(deptInfo)

        //注册累加器
        val deptAcc = new DeptInfoAccumator
        sc.register(deptAcc, "deptInfoAcc")


        val value = statisticsByDept(empInfo, bcDeptInfo, deptAcc)

        value.first()

        println("=" * 50)
        val deptStatistics = deptAcc.value
        deptStatistics.entrySet().forEach(new Consumer[JMap.Entry[Int, DeptStat]] {
            override def accept(t: JMap.Entry[Int, DeptStat]): Unit = {
                println(t)
            }
        })

        Thread.sleep(Int.MaxValue)
        sc.stop()
    }


    /**
      * 按部门统计部门的平均工资，最高工资，最低工资，总工资，总人数,并显示部门名称
      *
      * @param empInfo    员工信息
      * @param bcDeptInfo 部门信息广播变量
      */
    def statisticsByDept(empInfo: RDD[Emp],
                         bcDeptInfo: Broadcast[Map[Int, Dept]],
                         deptAcc: DeptInfoAccumator) = {

        empInfo.map(emp => {
            val deptInfo = bcDeptInfo.value
            if (deptInfo.contains(emp.deptno)) {
                deptAcc.add((emp, deptInfo.get(emp.deptno).get))
                (emp, deptInfo.get(emp.deptno).get.dname)
            } else {
                (emp, "unkown")
            }
        })
    }
}
