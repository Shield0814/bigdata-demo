package com.ljl.data.stack;

public class StackDemo {

    public static void main(String[] args) {
        Stack stack = new Stack(5);
        stack.push("7");
        stack.push("*");
        stack.push("8");
        stack.push("*");
        stack.push("9");

//        stack.show();
        stack.pop();
        stack.pop();
        stack.pop();
        stack.pop();
        stack.pop();
        stack.pop();
        stack.pop();
    }
}
