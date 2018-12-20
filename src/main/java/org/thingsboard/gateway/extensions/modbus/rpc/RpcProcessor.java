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
package org.thingsboard.gateway.extensions.modbus.rpc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.ghgande.j2mod.modbus.Modbus;
import com.ghgande.j2mod.modbus.ModbusException;
import com.ghgande.j2mod.modbus.facade.AbstractModbusMaster;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.gateway.extensions.modbus.ModbusDevice;
import org.thingsboard.gateway.extensions.modbus.ModbusDeviceAware;
import org.thingsboard.gateway.extensions.modbus.conf.mapping.TagValueMapping;
import org.thingsboard.gateway.extensions.modbus.util.ModbusUtils;
import org.thingsboard.gateway.service.RpcCommandListener;
import org.thingsboard.gateway.service.data.RpcCommandData;
import org.thingsboard.gateway.service.data.RpcCommandResponse;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.util.JsonTools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class RpcProcessor implements RpcCommandListener {
    private static final String RESPONSE_STATUS_OK = "ok";
    private static final String RESPONSE_FIELD_ERROR = "error";

    private static final String RPC_WRITE = "write";

    private final GatewayService gateway;
    private final AbstractModbusMaster client;
    private final ModbusDeviceAware deviceContainer;

    public RpcProcessor(GatewayService gateway, AbstractModbusMaster client, ModbusDeviceAware deviceContainer) {
        this.gateway = gateway;
        this.client = client;
        this.deviceContainer = deviceContainer;
    }

    @Override
    public void onRpcCommand(String deviceName, RpcCommandData command) {
        log.debug("RPC received: device='{}', command={}", deviceName, command);
        ModbusDevice device = deviceContainer.getDevice(deviceName);
        if (device == null) {
            log.warn("No device '{}' found for RPC {}", deviceName, command);
            gateway.onDeviceRpcResponse(createErrorResponse(command.getRequestId(),
                                        deviceName,
                                        String.format("No device '%s' found", deviceName)));
            return;
        }
        if (!RPC_WRITE.equals(command.getMethod())) {
            log.warn("Unknown RPC method '{}'", command.getMethod());
            gateway.onDeviceRpcResponse(createErrorResponse(command.getRequestId(),
                                                            deviceName,
                                                            String.format("Unsupported RPC method '%s'", command.getMethod())));
            return;
        }

        Map<String, String> results = new HashMap<>();
        List<TagValueMapping> mappings = JsonTools.fromString(command.getParams(), new TypeReference<List<TagValueMapping>>(){});

        for (TagValueMapping mapping : mappings) {
            try {
                switch (mapping.getFunctionCode()) {
                    case Modbus.WRITE_COIL:
                        log.trace("MBD[{}] executing 'write coil' for tag [{}]", device.getName(), mapping.getTag());
                        client.writeCoil(device.getUnitId(), mapping.getAddress(), (Boolean) mapping.getValue());
                        break;
                    case Modbus.WRITE_SINGLE_REGISTER: {
                        log.trace("MBD[{}] executing 'write single register' for tag [{}]", device.getName(), mapping.getTag());

                        byte[] formattedBytes = ModbusUtils.formatLsbBytes(ModbusUtils.toBytes(mapping.getValue(),
                                mapping.getRegisterCount()), mapping.getByteOrder());

                        client.writeSingleRegister(device.getUnitId(), mapping.getAddress(), ModbusUtils.toRegisters(formattedBytes)[0]);
                        break;
                    }
                    case Modbus.WRITE_MULTIPLE_REGISTERS: {
                        log.trace("MBD[{}] executing 'write multiple register' for tag [{}]", device.getName(), mapping.getTag());

                        byte[] formattedBytes;
                        if (mapping.getValue() instanceof String) {
                            formattedBytes = ModbusUtils.toBytes(mapping.getValue(), mapping.getRegisterCount());
                        } else {
                            formattedBytes = ModbusUtils.formatLsbBytes(
                                    ModbusUtils.toBytes(mapping.getValue(), mapping.getRegisterCount()), mapping.getByteOrder());
                        }
                        client.writeMultipleRegisters(device.getUnitId(), mapping.getAddress(), ModbusUtils.toRegisters(formattedBytes));
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Unsupported Modbus function " + mapping.getFunctionCode());
                }

                results.put(mapping.getTag(), RESPONSE_STATUS_OK);
                log.debug("MBD[{}] 'write multiple register' success for tag [{}], value [{}]",
                                device.getName(), mapping.getTag(), mapping.getValue());
            } catch (ModbusException e) {
                log.warn("MBD[{}] failed to execute {} Modbus functions for tag [{}]",
                            device.getName(), mapping.getFunctionCode(), mapping.getTag(), e);
                results.put(mapping.getTag(), e.getMessage());
            }
        }

        gateway.onDeviceRpcResponse(createResponse(command.getRequestId(), deviceName, results));
    }

    private RpcCommandResponse createErrorResponse(int requestId, String deviceName, String errorDescription) {
        RpcCommandResponse response = new RpcCommandResponse();
        response.setRequestId(requestId);
        response.setDeviceName(deviceName);
        Map<String, String> data = new HashMap<>();
        data.put(RESPONSE_FIELD_ERROR, errorDescription);
        response.setData(JsonTools.toString(data));
        return response;
    }

    private RpcCommandResponse createResponse(int requestId, String deviceName, Map<String, String> data) {
        RpcCommandResponse response = new RpcCommandResponse();
        response.setRequestId(requestId);
        response.setDeviceName(deviceName);
        response.setData(JsonTools.toString(data));
        return response;
    }
}
