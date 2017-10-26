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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.gateway.extensions.ExtensionService;
import org.thingsboard.gateway.extensions.file.DefaultFileTailService;
import org.thingsboard.gateway.extensions.http.DefaultHttpService;
import org.thingsboard.gateway.extensions.http.HttpService;
import org.thingsboard.gateway.extensions.mqtt.client.DefaultMqttClientService;
import org.thingsboard.gateway.extensions.opc.DefaultOpcUaService;
import org.thingsboard.gateway.service.conf.TbExtensionConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ashvayka on 29.09.17.
 */
@Slf4j
@Data
public class TenantServicesRegistry {

    private final GatewayService service;
    private final Map<String, ExtensionService> extensions;

    public TenantServicesRegistry(GatewayService service) {
        this.service = service;
        this.extensions = new HashMap<>();
    }

    public List<HttpService> updateExtensionConfiguration(String config) {
        ObjectMapper mapper = new ObjectMapper();
        List<HttpService> httpServices = new ArrayList<>();
        try {
            List<TbExtensionConfiguration> updatedExtensions = new ArrayList<>();
            for (JsonNode updatedExtension : mapper.readTree(config)) {
                updatedExtensions.add(mapper.treeToValue(updatedExtension, TbExtensionConfiguration.class));
            }
            for (String existingExtensionId : extensions.keySet()) {
                if (!extensionIdContainsInArray(existingExtensionId, updatedExtensions)) {
                    log.info("Destroying extension: [{}]", existingExtensionId);
                    extensions.get(existingExtensionId).destroy();
                    extensions.remove(existingExtensionId);
                }
            }
            for (TbExtensionConfiguration updatedExtension : updatedExtensions) {
                if (!extensions.containsKey(updatedExtension.getId())) {
                    log.info("Initializing extension: [{}][{}]", updatedExtension.getId(), updatedExtension.getType());
                    ExtensionService extension = createExtensionServiceByType(service, updatedExtension.getType());
                    extension.init(updatedExtension.getConfiguration());
                    if ("http".equals(updatedExtension.getType())) {
                        httpServices.add((HttpService) extension);
                    }
                    extensions.put(updatedExtension.getId(), extension);
                } else {
                    if(!updatedExtension.getConfiguration().equals(extensions.get(updatedExtension.getId()).getCurrentConfiguration())) {
                        log.info("Updating extension: [{}][{}]", updatedExtension.getId(), updatedExtension.getType());
                        extensions.get(updatedExtension.getId()).update(updatedExtension.getConfiguration());
                    }
                }
            }
        } catch (Exception e) {
            log.info("Failed to read configuration attribute", e);
        }
        return httpServices;
    }

    private boolean extensionIdContainsInArray(String extensionId, List<TbExtensionConfiguration> array) {
        for (TbExtensionConfiguration jsonNode : array) {
            if (jsonNode.getId().equalsIgnoreCase(extensionId)) {
                return true;
            }
        }
        return false;
    }

    private ExtensionService createExtensionServiceByType(GatewayService gateway, String type) {
        switch (type) {
            case "file":
                return new DefaultFileTailService(gateway);
            case "opc":
                return new DefaultOpcUaService(gateway);
            case "http":
                return new DefaultHttpService(gateway);
            case "mqtt":
                return new DefaultMqttClientService(gateway);
            default:
                throw new IllegalArgumentException("Extension: " + type + " is not supported!");
        }
    }
}
