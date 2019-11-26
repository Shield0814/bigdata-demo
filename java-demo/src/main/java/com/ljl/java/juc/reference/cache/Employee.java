package com.ljl.java.juc.reference.cache;

public class Employee {

    private String id;

    private String name;

    public Employee(String id) {
        this.id = id;
        this.name = String.valueOf(System.currentTimeMillis());
        //System.out.println("Employee.Employee() ..从数据库中或者其他资源获取对象");  
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}