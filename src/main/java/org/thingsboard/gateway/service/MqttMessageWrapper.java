package org.thingsboard.gateway.service;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by Valerii Sosliuk on 12/12/2017.
 */
@Data
public class MqttMessageWrapper implements Comparable<MqttMessageWrapper>, Serializable {

    private static final long serialVersionUID = -7371568402081050843L;

    private final String topic;
    private final String deviceId;
    private final int messageId;
    private final byte[] payload;
    private final long timestamp;


    @Override
    public int compareTo(MqttMessageWrapper that) {
        if (that.timestamp < this.timestamp) {
            return 1;
        }
        if (that.timestamp > this.timestamp) {
            return -1;
        }
        return 0;
    }

}
