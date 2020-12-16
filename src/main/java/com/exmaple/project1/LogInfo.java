package com.exmaple.project1;

import java.io.Serializable;

public class LogInfo implements Serializable {
    private long timeStamp;
    private long upTraffic;
    private long downTraffic;

    public LogInfo() {
    }

    public LogInfo(long timeStamp,long upTraffic, long downTraffic) {
        this.timeStamp = timeStamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }



    public long getUpTraffic() {
        return upTraffic;
    }

    public void setUpTraffic(long upTraffic) {
        this.upTraffic = upTraffic;
    }

    public long getDownTraffic() {
        return downTraffic;
    }

    public void setDownTraffic(long downTraffic) {
        this.downTraffic = downTraffic;
    }
}
