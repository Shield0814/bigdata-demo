package com.sitech.weibo.controller;


import com.sitech.weibo.common.ResultMessage;
import com.sitech.weibo.common.SysConfig;
import com.sitech.weibo.common.exception.NotSupportException;
import com.sitech.weibo.common.rowkey.JointGenerator;
import com.sitech.weibo.entity.WeiboEntity;
import com.sitech.weibo.service.WeiboService;

import java.io.IOException;
import java.util.List;

/**
 * 1) 创建命名空间以及表名的定义
 * 2) 创建微博内容表
 * 3) 创建用户关系表
 * 4) 创建用户微博内容接收邮件表
 * 5) 发布微博内容
 * 6) 添加关注用户
 * 7) 移除（取关）用户
 * 8) 获取关注的人的微博内容
 * 9) 测试
 */
public class WeiboController {

    private WeiboService weiboService = new WeiboService(new JointGenerator());

    //初始化namespace和表
    public void init() throws IOException {
        if ("true".equals(SysConfig.sysConfig.getProperty("init")))
            weiboService.initTable();
    }


    /**
     * 发表微博
     *
     * @param content 微博内容
     * @param userId  发表人
     * @return
     * @throws IOException
     * @throws NotSupportException
     */
    public ResultMessage publish(String content, String userId) throws IOException, NotSupportException {
        weiboService.publish(content, userId);
        return ResultMessage.get(true, "发表成功");
    }

    /**
     * 关注某人
     *
     * @param fansId 关注人
     * @param starId 被关注人
     * @return
     */
    public ResultMessage followSomeone(String fansId, String starId) throws IOException, NotSupportException {
        weiboService.followSomeone(fansId, starId);
        return ResultMessage.get(true, "关注成功");
    }

    /**
     * 取关某人
     *
     * @param fansId 粉丝标识
     * @param starId 要去关的用户
     * @return
     * @throws IOException
     * @throws NotSupportException
     */
    public ResultMessage unFollowSomeone(String fansId, String starId) throws IOException, NotSupportException {
        weiboService.unFollowSomeone(fansId, starId);
        return ResultMessage.get(true, "取关成功");
    }

    /**
     * 获取粉丝关注的所有明星的最近3条微博
     *
     * @param fansId
     * @return
     */
    public ResultMessage getStarsWeiboByFansId(String fansId) throws IOException, NotSupportException {
        List<WeiboEntity> content = weiboService.getStarsWeibo(fansId);
        return ResultMessage.get(true, "获取微博", content);
    }

    /**
     * 根据用户id获得用户微博信息
     *
     * @param userId 用户标识
     * @param limit  结果大小
     * @return
     * @throws IOException
     * @throws NotSupportException
     */
    public ResultMessage getWeiboByUserId(String userId, int limit) throws IOException, NotSupportException {
        List<WeiboEntity> content = weiboService.getWeiboByUserId(userId, limit);
        return ResultMessage.get(true, "获取微博", content);
    }

}
