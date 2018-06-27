/**
 * Copyright © 2017 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.gateway.service;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Valerii Sosliuk on 1/2/2018.
 */
@Data
@Builder
public class MqttPersistentMessage implements Serializable {

    private static final long serialVersionUID = -3133461476074777891L;

    private UUID id;
    private long timestamp;
    private String deviceId;
    private int messageId;
    private String topic;
    private byte[] payload;

    @Override
    public String toString() {
        return "{deviceId='" + deviceId + '\'' +
                ", payload=" + new String(payload) +
                ", timestamp=" + timestamp +
                ", topic='" + topic + '\'' +
                "id=" + id +
                ", messageId=" + messageId +
                '}';
    }
}
