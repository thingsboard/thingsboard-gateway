package org.thingsboard.gateway.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.function.Consumer;

/**
 * Created by Valerii Sosliuk on 12/12/2017.
 */
@Data
@AllArgsConstructor
public class MqttMessageWrapper implements Comparable<MqttMessageWrapper>{

    private String topic;
    private MqttMessage msg;
    private Consumer<IMqttToken> onSuccess;
    private Consumer<Throwable> onFailure;
    private long timestamp;

    @Override
    public int compareTo(MqttMessageWrapper that) {
        if (that.timestamp > this.timestamp) {
            return 1;
        }
        if (that.timestamp < this.timestamp) {
            return -1;
        }
        return 0;
    }
}
