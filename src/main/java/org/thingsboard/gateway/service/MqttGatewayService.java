/**
 * Copyright © ${project.inceptionYear}-2017 The Thingsboard Authors
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

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.internal.security.SSLSocketFactoryFactory;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by ashvayka on 16.01.17.
 */
@Service
@Slf4j
public class MqttGatewayService implements GatewayService {

    private final UUID clientId = UUID.randomUUID();

    @Autowired
    private MqttGatewayConfiguration configuration;

    private MqttClient tbClient;

    @PostConstruct
    public void init() throws Exception {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);

        MqttGatewaySecurityConfiguration security = configuration.getSecurity();
        if (security.isSsl()) {
            Properties sslProperties = new Properties();
            sslProperties.put(SSLSocketFactoryFactory.KEYSTORE, security.getKeystore());
            sslProperties.put(SSLSocketFactoryFactory.KEYSTOREPWD, security.getKeystorePassword());
            sslProperties.put(SSLSocketFactoryFactory.KEYSTORETYPE, "JKS");
            sslProperties.put(SSLSocketFactoryFactory.TRUSTSTORE, security.getTruststore());
            sslProperties.put(SSLSocketFactoryFactory.TRUSTSTOREPWD, security.getTruststorePassword());
            sslProperties.put(SSLSocketFactoryFactory.TRUSTSTORETYPE, "JKS");
            sslProperties.put(SSLSocketFactoryFactory.CLIENTAUTH, true);
            options.setSSLProperties(sslProperties);
        } else {
            options.setUserName(security.getAccessToken());
        }

        tbClient = new MqttClient((security.isSsl() ? "ssl" : "tcp") + "://" + configuration.getHost() + ":" + configuration.getPort(),
                clientId.toString(), new MemoryPersistence());
        tbClient.connect(options);
    }

    @Override
    public boolean getOrCreateDevice(String deviceName) {
        log.info("[{}] Device Created!", deviceName);
        return true;
    }

    @Override
    public void connect(String deviceName) {
        log.info("[{}] Device Connected!", deviceName);
    }

    @Override
    public void disconnect(String deviceName) {
        log.info("[{}] Device Disconnected!", deviceName);
    }

    @Override
    public void onDeviceAttributesUpdate(String deviceName, List<KvEntry> attributes) {
        log.info("[{}] Updating device attributes: {}", deviceName, attributes);
    }

    @Override
    public void onDeviceTimeseriesUpdate(String deviceName, List<TsKvEntry> timeseries) {
        log.info("[{}] Updating timeseries attributes: {}", deviceName, timeseries);
    }
}