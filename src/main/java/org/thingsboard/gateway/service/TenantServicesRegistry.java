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
        JsonNode extensionsArray;
        try {
            extensionsArray = mapper.readTree(config);
            for (String id : extensions.keySet()) {
                for (int i = 0; i < extensionsArray.size(); i++) {
                    if (id.equals(extensionsArray.get(i).get("id").asText())) {
                        break;
                    } else {
                        if (i == extensionsArray.size() - 1) {
                            log.info("Destroying extension: [{}]", extensionsArray.get(i).get("type").asText());
                            extensions.get(id).destroy();
                            extensions.remove(id);
                        }
                    }
                }
            }
            for (int i = 0; i < extensionsArray.size(); i++) {
                TbExtensionConfiguration extensionConfiguration = mapper.treeToValue(extensionsArray.get(i), TbExtensionConfiguration.class);
                if (extensions.containsKey(extensionConfiguration.getId())) {
                    log.info("Updating extension configuration: [{}]", extensionConfiguration.getType());
                    extensions.get(extensionConfiguration.getId()).update(extensionConfiguration.getConfiguration());
                } else {
                    log.info("Initializing extension: [{}]", extensionConfiguration.getType());
                    ExtensionService extension = createExtensionServiceByType(service, extensionConfiguration.getType());
                    extension.init(extensionConfiguration.getConfiguration());
                    if (extensionConfiguration.getType().equals("http")) {
                        httpServices.add((HttpService) extension);
                    }
                    extensions.put(extensionConfiguration.getId(), extension);
                }
            }
        } catch (Exception e) {
            log.info("Failed to read configuration attribute", e);
        }
        return httpServices;
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
