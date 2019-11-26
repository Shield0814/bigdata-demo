package com.ljl.scala.contextBound

//上下文界定也是使用隐式转换的的一种方式，他会在上下文查找对应的隐式值或隐式函数
object ContextBoundDemo {


}

//方式1
class CompareComm4[T: Ordering](obj1: T, obj2: T)(implicit comparetor: Ordering[T]) {
    def geatter = if (comparetor.compare(obj1, obj2) > 0) obj1 else obj2
}

//方式2,将隐式参数放到方法内
class CompareComm5[T: Ordering](o1: T, o2: T) {
    def geatter = {
        def f1(implicit cmptor: Ordering[T]) = cmptor.compare(o1, o2)

        if (f1 > 0) o1 else o2
    }
}

//方式3,使用implicitly语法糖，最简单(推荐使用)
class CompareComm6[T: Ordering](o1: T, o2: T) {
    def geatter = {
        //这句话就是会发生隐式转换，获取到隐式值 personComparetor
        val comparetor = implicitly[Ordering[T]]
        println("CompareComm6 comparetor" + comparetor.hashCode())
        if (comparetor.compare(o1, o2) > 0) o1 else o2
    }
}

//一个普通的Person类
class Person(val name: String, val age: Int) {
    override def toString = this.name + "\t" + this.age
}
