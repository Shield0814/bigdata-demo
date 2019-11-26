package com.sitech.hotitems.entity;

/**
 * 品类商品维度统计信息
 */
public class CategoryItemStat {

    //品类 id
    private int categoryId;

    //商品id
    private long itemId;

    //累加次数
    private long count;

    private long windowEndTs;

    public CategoryItemStat() {
        super();
    }

    public CategoryItemStat(int categoryId, long itemId, long count) {
        super();
        this.categoryId = categoryId;
        this.itemId = itemId;
        this.count = count;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getWindowEndTs() {
        return windowEndTs;
    }

    public void setWindowEndTs(long windowEndTs) {
        this.windowEndTs = windowEndTs;
    }

    @Override
    public String toString() {
        return "CategoryItemStat{" +
                categoryId +
                "_" + itemId +
                ", " + count +
                ", " + windowEndTs +
                '}';
    }
}
