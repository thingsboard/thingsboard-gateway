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

import org.thingsboard.gateway.extensions.opc.OpcUaDevice;
import org.thingsboard.gateway.service.data.AttributesUpdateSubscription;
import org.thingsboard.gateway.service.data.RpcCommandSubscription;
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
    void onDeviceConnect(String deviceName);

    /**
     * Inform gateway service that device is disconnected
     * @param deviceName
     */
    void onDeviceDisconnect(String deviceName);

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
    void onDeviceTelemetry(String deviceName, List<TsKvEntry> telemetry);

    /**
     * Report response from device to the server-side RPC call from Thingsboard
     * @param deviceName - the device name
     * @param requestId - the request id
     * @param payload - the payload
     */
    void onDeviceRpcResponse(String deviceName, String requestId, String payload);

    /**
     * Subscribe to attribute updates from Thingsboard
     * @param subscription - the subscription
     * @return true if successful, false if already subscribed
     *
     */
    boolean subscribe(AttributesUpdateSubscription subscription);

    /**
     * Subscribe to server-side rpc commands from Thingsboard
     * @param subscription - the subscription
     * @return true if successful, false if already subscribed
     */
    boolean subscribe(RpcCommandSubscription subscription);

    /**
     * Unsubscribe to attribute updates from Thingsboard
     * @param subscription - the subscription
     * @return true if successful, false if already unsubscribed
     */
    boolean unsubscribe(AttributesUpdateSubscription subscription);

    /**
     * Unsubscribe to server-side rpc commands from Thingsboard
     * @param subscription - the subscription
     * @return true if successful, false if already unsubscribed
     */
    boolean unsubscribe(RpcCommandSubscription subscription);

    /**
     * Report generic error from one of gateway components
     * @param e - the error
     */
    void onError(Exception e);

    /**
     * Report error related to device
     * @param deviceName - the device name
     * @param e - the error
     */
    void onError(String deviceName, Exception e);
}
