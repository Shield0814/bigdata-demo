package com.ljl.data.hashtable;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class HashTableDemo {

    static volatile AtomicInteger id = new AtomicInteger(1);
    static HashTable empHashTable = new HashTable(10);

    public static void main(String[] args) {

        printMenu();
        Scanner scanner = new Scanner(System.in);
        boolean isRunning = true;
        while (isRunning) {
            String key = scanner.next();
            switch (key) {
                case "add":
                    System.out.println("请输入员工姓名:");
                    String name = scanner.next();
                    addEmp(name);
                    break;
                case "list":
                    System.out.println("==============================");
                    empHashTable.list();
                    System.out.println("==============================");
                    break;
                case "find":
                    System.out.println("请输入员工编号:");
                    String id = scanner.next();
                    findById(id);
                    break;
                case "exit":
                    isRunning = false;
                    break;
                default:
                    System.out.println("输入错误");
                    printMenu();
                    break;
            }
        }
    }

    /**
     * 添加员工
     *
     * @param name
     */
    public static void addEmp(String name) {
        String id = String.valueOf(HashTableDemo.id.getAndIncrement());
        HashTable.Emp emp = empHashTable.new Emp(id, name, null);
        empHashTable.addEmp(emp);
    }

    public static void findById(String id) {
        HashTable.Emp emp = empHashTable.findById(id);
        if (emp != null) {
            System.out.println(emp);
        } else {
            System.out.println("找不到");
        }
    }


    public static void printMenu() {
        System.out.println("=================1.add 添加员工==================");
        System.out.println("=================1.list 查看员工==================");
        System.out.println("=================1.exit 退出==================");
    }


}
