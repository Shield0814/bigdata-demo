package com.sitech.weibo.entity;

public class WeiboEntity {

    private String weiboId;

    private String weiboContent;

    private String publishTime;

    public WeiboEntity(String weiboId, String weiboContent) {
        this.weiboId = weiboId;
        this.weiboContent = weiboContent;
    }

    public WeiboEntity(String weiboId, String weiboContent, String publishTime) {
        this.weiboId = weiboId;
        this.weiboContent = weiboContent;
        this.publishTime = publishTime;
    }

    public WeiboEntity() {
    }

    public String getWeiboId() {
        return weiboId;
    }

    public void setWeiboId(String weiboId) {
        this.weiboId = weiboId;
    }

    public String getWeiboContent() {
        return weiboContent;
    }

    public void setWeiboContent(String weiboContent) {
        this.weiboContent = weiboContent;
    }

    @Override
    public String toString() {
        return "WeiboEntity{" +
                "weiboId='" + weiboId + '\'' +
                ", weiboContent='" + weiboContent + '\'' +
                ", publishTime='" + publishTime + '\'' +
                '}';
    }
}
