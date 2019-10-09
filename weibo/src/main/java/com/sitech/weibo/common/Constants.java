package com.sitech.weibo.common;

public class Constants {

    //微博项目数据存储namespace
    public static final String NAMESPACE_NAME = "weibo";

    //微博表名称
    public static final String WEIBO_TABLE_NAME = NAMESPACE_NAME + ":weibo";

    //用户关系表名称
    public static final String RELATIONSHIP_TABLE_NAME = NAMESPACE_NAME + ":relationship";

    //用户微博内容接收邮件表
    public static final String INBOX_TABLE_NAME = NAMESPACE_NAME + ":inbox";

    //微博表列族列表，多个逗号分割
    public static final String WEIBO_FAMILYS_NAME = "data";

    //用户关系表列族列表，多个逗号分割
    public static final String RELATIONSHIP_FAMILYS_NAME = "data";

    //用户微博内容接收邮件表列族信息，多个逗号分割
    public static final String INBOX_FAMILYS_NAME = "data";

    //用户微博内容接收邮件表最大版本
    public static final int INBOX_VERSIONS = 3;

    //删除标识，1标识已删除，0标识未删除
    public static final String DELETE_FLAG = "deleted";


}
