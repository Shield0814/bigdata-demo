package com.ljl.scala.collection

/**
  * 元组是一个不可变量，元组生成后不能进行修改操作
  */
object TupleDemo {


    def main(args: Array[String]): Unit = {

        //1. 元组的创建
        val tuple1 = Tuple2(1, 2)
        val tuple2 = (1, 2, 3, 4, 5, "a")

        //2. 元组的访问
        println(tuple1._2)

        //3. 元组的遍历
        for (ele <- tuple2.productIterator) {
            println(s"元组元素：$ele")
        }


    }


}
