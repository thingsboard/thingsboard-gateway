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
package org.thingsboard.gateway.service.conf;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * Created by ashvayka on 24.01.17.
 */
@Configuration
@ConfigurationProperties(prefix = "gateway.persistence")
@Data
@Slf4j
public class TbPersistenceConfiguration {

    private String type;
    private String path;
    private int bufferSize;

    public MqttClientPersistence getPersistence() {
        if (StringUtils.isEmpty(type) || type.equals("memory")) {
            log.info("Initializing default memory persistence!");
            return new MemoryPersistence();
        } else if (type.equals("file")) {
            if (StringUtils.isEmpty(path)) {
                log.info("Initializing default file persistence!");
                return new MqttDefaultFilePersistence();
            } else {
                log.info("Initializing file persistence using directory: {}", path);
                return new MqttDefaultFilePersistence(path);
            }
        } else {
            log.error("Unknown persistence option: {}. Only 'memory' and 'file' are supported at the moment!", type);
            throw new IllegalArgumentException("Unknown persistence option: " + type + "!");
        }
    }
}
