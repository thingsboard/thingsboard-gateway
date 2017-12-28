package org.thingsboard.gateway.dao;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.GenericGenerator;
import org.springframework.beans.factory.annotation.Autowired;

import javax.persistence.*;
import java.util.UUID;


/**
 * Created by Valerii Sosliuk on 12/24/2017.
 */

@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "MQTT_MESSAGE")
public class PersistentMqttMessage {

    @Id
    @GeneratedValue(generator = "UUID")
    @GenericGenerator(
            name = "UUID",
            strategy = "org.hibernate.id.UUIDGenerator"
    )
    private UUID id;
    private long timestamp;
    private String deviceId;
    private int messageId;
    private String topic;
    private byte[] payload;

    public PersistentMqttMessage(PersistentMqttMessage that) {
        this.timestamp = that.timestamp;
        this.deviceId = that.deviceId;
        this.messageId = that.messageId;
        this.topic = that.topic;
        this.payload = that.payload;
    }
}
