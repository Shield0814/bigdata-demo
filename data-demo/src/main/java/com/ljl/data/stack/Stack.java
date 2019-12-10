package com.ljl.data.stack;

public class Stack<T> {

    //栈深度
    int stackDepth;

    //栈顶
    int top = -1;

    //栈数据
    Object[] data;

    public Stack(int stackDepth) {
        this.stackDepth = stackDepth;
        this.data = new Object[this.stackDepth];
    }

    /**
     * 压栈操作
     *
     * @param element
     */
    public boolean push(T element) {
        if (isFull()) {
            //如果栈已满，则不能再进行压栈操作
            System.out.println("栈已满，不能再进行压栈操作");
            return false;
        } else {
            top += 1;
            data[top] = element;
            return true;
        }
    }

    /**
     * 出栈操作
     *
     * @return
     */
    public T pop() {
        if (!isEmpty()) {
            T element = (T) data[top];
            top -= 1;
            return element;
        }
        System.out.println("栈为空，返回结果为null");
        return null;
    }


    /**
     * 栈是否为空
     *
     * @return
     */
    public boolean isEmpty() {
        return top == -1;
    }

    /**
     * 栈是否已满
     *
     * @return
     */
    public boolean isFull() {
        return top == stackDepth - 1;
    }

    /**
     * 打印栈中的元素
     */
    public void show() {
        for (int i = 0; i <= top; i++) {
            System.out.println(data[i]);
        }
    }


}
