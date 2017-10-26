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
package org.thingsboard.gateway.extensions.http;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.thingsboard.gateway.extensions.ExtensionUpdate;
import org.thingsboard.gateway.extensions.http.conf.HttpConfiguration;
import org.thingsboard.gateway.extensions.http.conf.HttpConverterConfiguration;
import org.thingsboard.gateway.extensions.http.conf.mapping.HttpDeviceDataConverter;
import org.thingsboard.gateway.service.conf.TbExtensionConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.service.gateway.MqttDeliveryFuture;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.util.ConfigurationTools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class DefaultHttpService extends ExtensionUpdate implements HttpService {

    private static final int OPERATION_TIMEOUT_IN_SEC = 10;

    private final GatewayService gateway;
    private TbExtensionConfiguration currentConfiguration;
    private Map<String, HttpConverterConfiguration> httpConverterConfigurations;

    public DefaultHttpService(GatewayService gateway) {
        this.gateway = gateway;
    }

    @Override
    public TbExtensionConfiguration getCurrentConfiguration() {
        return currentConfiguration;
    }

    @Override
    public void init(TbExtensionConfiguration configurationNode) throws IOException {
        currentConfiguration = configurationNode;
        HttpConfiguration configuration;
        try {
            configuration = ConfigurationTools.readConfiguration(configurationNode.getConfiguration(), HttpConfiguration.class);
            if (configuration.getConverterConfigurations() != null) {
                httpConverterConfigurations = configuration
                        .getConverterConfigurations()
                        .stream()
                        .collect(Collectors.toMap(HttpConverterConfiguration::getConverterId, Function.identity()));
            } else {
                httpConverterConfigurations = configuration
                        .getDeviceTypeConfigurations()
                        .stream()
                        .collect(Collectors.toMap(HttpConverterConfiguration::getDeviceTypeId, Function.identity()));
            }
        } catch (IOException e) {
            log.error("[{}] Http service configuration failed!", gateway.getTenantLabel(), e);
            throw e;
        }
    }

    @Override
    public void destroy() throws Exception { }

    @Override
    public void processRequest(String converterId, String token, String body) throws Exception {
        log.trace("[{}] Processing request body [{}] for converterId [{}] and token [{}]", gateway.getTenantLabel(), body, converterId, token);
        HttpConverterConfiguration configuration = httpConverterConfigurations.get(converterId);
        if (configuration != null) {
            if (StringUtils.isEmpty(configuration.getToken()) || configuration.getToken().equals(token)) {
                processBody(body, configuration);
            } else {
                log.error("[{}] Request token [{}] for converter id [{}] doesn't match configuration token!", gateway.getTenantLabel(), token, converterId);
                throw new SecurityException("Request token [" + token + "] for converter id [" + converterId + "] doesn't match configuration token!");
            }
        }
    }

    private void processBody(String body, HttpConverterConfiguration configuration) throws Exception {
        for (HttpDeviceDataConverter converter : configuration.getConverters()) {
            if (converter.isApplicable(body)) {
                DeviceData dd = converter.parseBody(body);
                if (dd != null) {
                    waitWithTimeout(gateway.onDeviceConnect(dd.getName(), dd.getType()));
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
                    log.error("[{}] DeviceData is null. Body [{}] was not parsed successfully!", gateway.getTenantLabel(), body);
                    throw new IllegalArgumentException("Device Data is null. Body [" + body + "] was not parsed successfully!");
                }
            }
        }
    }

    private void waitWithTimeout(Future future) throws Exception {
        future.get(OPERATION_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    }
}
