package flume.util;

import java.util.Date;

public class O2mLogEntry {
    private Date beginTime;
    private Date endTime;
    private String interfaceId;
    private String serviceName;
    private String methodName;
    private String sender;
    private String receiver;
    private String messageId;
    private String key;
    private String note;
    private int failFlag;
    private String errorCode;
    private String requestStr;
    private String responseStr;

    public O2mLogEntry() {
    }
}
