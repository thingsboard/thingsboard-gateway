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

import org.thingsboard.gateway.extensions.opc.OpcUaDevice;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;

import java.util.List;

/**
 * Created by ashvayka on 16.01.17.
 */
public interface GatewayService {

    /**
     * Inform gateway service that device is connected
     * @param deviceName
     */
    void connect(String deviceName);

    /**
     * Inform gateway service that device is disconnected
     * @param deviceName
     */
    void disconnect(String deviceName);

    /**
     * Report device attributes change to Thingsboard
     * @param deviceName - the device name
     * @param attributes - the attribute values list
     */
    void onDeviceAttributesUpdate(String deviceName, List<KvEntry> attributes);

    /**
     * Report device telemetry to Thingsboard
     * @param deviceName - the device name
     * @param telemetry - the telemetry values list
     */
    void onDeviceTimeseriesUpdate(String deviceName, List<TsKvEntry> telemetry);
}
