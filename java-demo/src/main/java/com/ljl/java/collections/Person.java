package com.ljl.java.collections;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class Person {

    private String name;

    private Date birthday;

    private int age;

    private float sal;

    private long comm;

    private double timestamp;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person persion = (Person) o;
        return age == persion.age &&
                Float.compare(persion.sal, sal) == 0 &&
                comm == persion.comm &&
                Double.compare(persion.timestamp, timestamp) == 0 &&
                Objects.equals(name, persion.name) &&
                Objects.equals(birthday, persion.birthday);
    }

    @Override
    public int hashCode() {


        return Objects.hash(name, birthday, age, sal, comm, timestamp);
    }


    public static void main(String[] args) {

        new LinkedHashMap<String, String>(16, (float) 0.75, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return super.removeEldestEntry(eldest);
            }
        };


    }
}
