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
package org.thingsboard.gateway.extensions.mqtt.client.conf;

import lombok.Data;
import org.thingsboard.gateway.extensions.mqtt.client.conf.credentials.MqttClientCredentials;
import org.thingsboard.gateway.extensions.mqtt.client.conf.mapping.*;

import java.util.List;

/**
 * Created by ashvayka on 23.01.17.
 */
@Data
public class MqttBrokerConfiguration {
    private String host;
    private int port;
    private boolean ssl;
    private String clientId;
    private String truststore;
    private String truststorePassword;
    private long retryInterval;
    private MqttClientCredentials credentials;
    private List<MqttTopicMapping> mapping;
    private List<DeviceConnectMapping> connectRequests;
    private List<DeviceDisconnectMapping> disconnectRequests;
    private List<AttributeRequestsMapping> attributeRequests;
    private List<AttributeUpdatesMapping> attributeUpdates;
    private List<ServerSideRpcMapping> serverSideRpc;
}
