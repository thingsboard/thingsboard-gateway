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
package org.thingsboard.gateway.extensions.sigfox;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.thingsboard.gateway.extensions.sigfox.conf.SigfoxConfiguration;
import org.thingsboard.gateway.extensions.sigfox.conf.SigfoxDeviceTypeConfiguration;
import org.thingsboard.gateway.service.GatewayService;
import org.thingsboard.gateway.service.MqttDeliveryFuture;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.util.ConfigurationTools;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
@ConditionalOnProperty(prefix = "sigfox", value = "enabled", havingValue = "true")
public class DefaultSigfoxService implements SigfoxService {

    private static final int OPERATION_TIMEOUT_IN_SEC = 10;

    @Value("${sigfox.configuration}")
    private String configurationFile;

    @Autowired
    private GatewayService gateway;

    private Map<String, SigfoxDeviceTypeConfiguration> sigfoxDeviceTypeConfigurations;

    @PostConstruct
    public void init() throws IOException {
        SigfoxConfiguration configuration;
        try {
            configuration = ConfigurationTools.readConfiguration(configurationFile, SigfoxConfiguration.class);
            sigfoxDeviceTypeConfigurations = configuration
                    .getDeviceTypeConfigurations()
                    .stream()
                    .collect(Collectors.toMap(SigfoxDeviceTypeConfiguration::getDeviceTypeId, Function.identity()));
        } catch (IOException e) {
            log.error("Sigfox service configuration failed!", e);
            throw e;
        }
    }

    @Override
    public void processRequest(String deviceTypeId, String token, String body) throws Exception {
        log.trace("Processing request body [{}] for deviceTypeId [{}] and token [{}]", body, deviceTypeId, token);
        SigfoxDeviceTypeConfiguration configuration = sigfoxDeviceTypeConfigurations.get(deviceTypeId);
        if (configuration != null) {
            if (configuration.getToken().equals(token)) {
                processBody(body, configuration);
            } else {
                log.error("Request token [{}] for device type id [{}] doesn't match configuration token!", token, deviceTypeId);
                throw new SecurityException("Request token [" + token + "] for device type id [" + deviceTypeId + "] doesn't match configuration token!");
            }
        } else {
            log.error("No configuration found for device type id [{}]. Please add configuration for this device type id!", deviceTypeId);
            throw new IllegalArgumentException("No configuration found for device type id [" + deviceTypeId + "]. Please add configuration for this device type id!");
        }
    }

    private void processBody(String body, SigfoxDeviceTypeConfiguration configuration) throws Exception {
        DeviceData dd = configuration.getConverter().parseBody(body);
        if (dd != null) {
            waitWithTimeout(gateway.onDeviceConnect(dd.getName()));
            List<MqttDeliveryFuture> futures = new ArrayList<>();
            if (!dd.getAttributes().isEmpty()) {
                futures.add(gateway.onDeviceAttributesUpdate(dd.getName(), dd.getAttributes()));
            }
            if (!dd.getTelemetry().isEmpty()) {
                futures.add(gateway.onDeviceTelemetry(dd.getName(), dd.getTelemetry()));
            }
            for (Future future : futures) {
                waitWithTimeout(future);
            }
            Optional<MqttDeliveryFuture> future = gateway.onDeviceDisconnect(dd.getName());
            if (future.isPresent()) {
                waitWithTimeout(future.get());
            }
        } else {
            log.error("DeviceData is null. Body [{}] was not parsed successfully!", body);
            throw new IllegalArgumentException("Device Data is null. Body [" + body + "] was not parsed successfully!");
        }
    }

    private void waitWithTimeout(Future future) throws Exception {
        future.get(OPERATION_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    }
}
