package com.sitech.weibo.common;

public class ResultMessage {

    //标记是否成功
    private boolean result;

    //结果消息
    private String msg;

    private Object content;

    private ResultMessage(boolean result, String msg) {
        this.result = result;
        this.msg = msg;
    }

    public boolean isResult() {
        return result;
    }

    public void setResult(boolean result) {
        this.result = result;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    private ResultMessage(boolean result, String msg, Object content) {
        this.result = result;
        this.msg = msg;
        this.content = content;
    }

    public static ResultMessage get(boolean result, String msg) {
        return new ResultMessage(result, msg);
    }

    public static ResultMessage get(boolean result, String msg, Object content) {
        return new ResultMessage(result, msg, content);
    }


    @Override
    public String toString() {
        return "ResultMessage{" +
                "result=" + result +
                ", msg='" + msg + '\'' +
                '}';
    }
}
