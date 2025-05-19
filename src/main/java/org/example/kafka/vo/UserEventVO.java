package org.example.kafka.vo;

public class UserEventVO {
    private String timeStamp;
    private String userAgent;
    private String colorName;
    private String userName;


    public UserEventVO(String timeStamp, String userAgent, String colorName, String userName) {
        this.timeStamp = timeStamp;
        this.userAgent = userAgent;
        this.colorName = colorName;
        this.userName = userName;
    }
}
