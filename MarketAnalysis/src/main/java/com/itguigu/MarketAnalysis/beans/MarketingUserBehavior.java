package com.itguigu.MarketAnalysis.beans;

public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timstamp;

    public MarketingUserBehavior() {
    }

    public MarketingUserBehavior(Long userId, String behavior, String channel, Long timstamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timstamp = timstamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getTimstamp() {
        return timstamp;
    }

    public void setTimstamp(Long timstamp) {
        this.timstamp = timstamp;
    }

    @Override
    public String toString() {
        return "MarketingUserBehavior{" +
                "userId=" + userId +
                ", behavior='" + behavior + '\'' +
                ", channel='" + channel + '\'' +
                ", timstamp=" + timstamp +
                '}';
    }
}
