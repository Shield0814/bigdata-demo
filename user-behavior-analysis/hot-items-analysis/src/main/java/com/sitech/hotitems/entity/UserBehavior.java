package com.sitech.hotitems.entity;


/**
 * 用户行为信息
 */
public class UserBehavior {

    //加密后的用户id
    private long userId;

    //加密后的商品id
    private long itemId;

    //加密后的分类id
    private int categoryId;

    //用户行为: pv[浏览],buy[下单],cart[加入购物车],fav[收藏],
    private String behavior;

    //行为发生的时间：输入数据单位为秒
    private long timestamp;

    public UserBehavior() {
        super();
    }

    public UserBehavior(long userId, long itemId, int categoryId, String behavior, long timestamp) {
        super();
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
