package com.ljl.java.polymorphism;


public class PolymorphismDemo {

    public static void main(String[] args) {
        //多态中，成员变量，静态方法看左边；非静态方法：编译看左边，运行看右边
//        java中的动态绑定规则：
//        当调用一个对象的成员方法时，会将方法和对象的实际内存地址进行绑定，然后调用
//        成员属性的访问没有动态绑定的概念，即在哪里声明，在哪里使用
        Super s1 = new Sub();
        Sub s2 = new Sub();
//        成员属性的访问没有动态绑定的概念，即在哪里声明，在哪里使用
        //成员变量
        System.out.println("s1.name>" + s1.name); // super
        System.out.println("s2.name>" + s2.name); // sub
        System.out.println("==========================");
        //静态变量
        System.out.println("s1.age>" + s1.age); //
        System.out.println("s2.age>" + s2.age);
        System.out.println("==========================");
//        当调用一个对象的成员方法时，会将方法和对象的实际内存地址进行绑定，然后调用
        //成员方法
        s1.commonMethod();  // sub
        s2.commonMethod(); // sub

        System.out.println("==========================");
        //静态方法: 不能被重写
        s1.staticMethod();  //
        s2.staticMethod(); //


    }

}


class Super {
    String name = "super";
    static int age = 20;

    protected static void staticMethod() {
        System.out.println("static Method In Super class");
    }

    public void commonMethod() {
        System.out.println("common Method In Super class");
    }

}

class Sub extends Super {
    String name = "sub";
    static int age = 10;


    protected static void staticMethod() {
        System.out.println("static Method In sub class");
    }

    @Override
    public void commonMethod() {
        System.out.println("common Method In sub class");
    }
}