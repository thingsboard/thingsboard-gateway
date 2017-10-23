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

import lombok.Data;
import org.thingsboard.gateway.extensions.ExtensionService;
import org.thingsboard.gateway.extensions.file.DefaultFileTailService;
import org.thingsboard.gateway.extensions.http.DefaultHttpService;
import org.thingsboard.gateway.extensions.mqtt.client.DefaultMqttClientService;
import org.thingsboard.gateway.extensions.opc.DefaultOpcUaService;
import org.thingsboard.gateway.service.conf.TbExtensionConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ashvayka on 29.09.17.
 */
@Data
public class TenantServicesRegistry {

    private final GatewayService service;
    private final List<ExtensionService> extensions;

    public TenantServicesRegistry(GatewayService service) {
        this.service = service;
        this.extensions = new ArrayList<>();
    }

    public void updateExtensionConfiguration(String c) {
        //TODO: implement
    }

    private ExtensionService createExtensionServiceByType(GatewayService gateway, TbExtensionConfiguration configuration) {
        switch (configuration.getType()) {
            case "file":
                return new DefaultFileTailService(gateway);
            case "opc":
                return new DefaultOpcUaService(gateway);
            case "http":
                return new DefaultHttpService(gateway);
            case "mqtt":
                return new DefaultMqttClientService(gateway);
            default:
                throw new IllegalArgumentException("Extension: " + configuration.getType() + " is not supported!");
        }
    }
}
