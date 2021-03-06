package com.atguigu.otitem_analysis.beans;

public class UserBehavior {
    //定义私有属性
    // 用户id
    private Long userId;
    // 商品id
    private Long itemId;
    // 商品类别id
    private Integer cateoryId;
    // 用户行为
    private String behavior;
    // 时间戳
    private Long timestamp;


    public UserBehavior() {
    }

    public UserBehavior(Long userId, Long itemId, Integer cateoryId, String behavior, Long timestamp) {

        this.userId = userId;
        this.itemId = itemId;
        this.cateoryId = cateoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Integer getCateoryId() {
        return cateoryId;
    }

    public void setCateoryId(Integer cateoryId) {
        this.cateoryId = cateoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", cateoryId=" + cateoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
