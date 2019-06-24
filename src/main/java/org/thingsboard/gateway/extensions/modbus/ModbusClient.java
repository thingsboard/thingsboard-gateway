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
package org.thingsboard.gateway.extensions.modbus;

import com.ghgande.j2mod.modbus.Modbus;
import com.ghgande.j2mod.modbus.ModbusException;
import com.ghgande.j2mod.modbus.facade.AbstractModbusMaster;
import com.ghgande.j2mod.modbus.facade.ModbusSerialMaster;
import com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
import com.ghgande.j2mod.modbus.facade.ModbusUDPMaster;
import com.ghgande.j2mod.modbus.io.ModbusTransaction;
import com.ghgande.j2mod.modbus.msg.*;
import com.ghgande.j2mod.modbus.util.SerialParameters;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.gateway.extensions.modbus.conf.ModbusExtensionConstants;
import org.thingsboard.gateway.extensions.modbus.conf.ModbusServerConfiguration;
import org.thingsboard.gateway.extensions.modbus.conf.mapping.PollingTagMapping;
import org.thingsboard.gateway.extensions.modbus.conf.transport.*;
import org.thingsboard.gateway.extensions.modbus.rpc.RpcProcessor;
import org.thingsboard.gateway.service.data.RpcCommandSubscription;
import org.thingsboard.gateway.service.gateway.GatewayService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class ModbusClient implements ModbusDeviceAware {

    private GatewayService gateway;
    private ModbusServerConfiguration configuration;

    private AbstractModbusMaster client;
    private ModbusTransaction transaction;
    private String serverName;

    private Map<String, ModbusDevice> devices;

    private ScheduledExecutorService executor;
    private RpcProcessor rpcProcessor;

    ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

    private Object connectLock = new Object();

    public ModbusClient(GatewayService gateway, ModbusServerConfiguration configuration) {
        this.gateway = gateway;
        this.configuration = configuration;
        this.client = createClient(this.configuration.getTransport());
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.devices = this.configuration.getDevices().stream().collect(Collectors.toMap(c -> c.getDeviceName(), c -> new ModbusDevice(c)));
        this.serverName = createServerName(this.configuration.getTransport());
        this.rpcProcessor = new RpcProcessor(this.gateway, this.client, this);
    }

    @Override
    public ModbusDevice getDevice(String deviceName) {
        return devices.get(deviceName);
    }

    private String createServerName(ModbusTransportConfiguration configuration) {
        if (configuration instanceof ModbusIpTransportConfiguration) {
            ModbusIpTransportConfiguration ipConf = (ModbusIpTransportConfiguration) configuration;
            return ipConf.getHost() + ":" + ipConf.getPort();
        } else if (configuration instanceof ModbusRtuTransportConfiguration) {
            return ((ModbusRtuTransportConfiguration) configuration).getPortName();
        }

        throw new IllegalArgumentException("Unknown Modbus transport " + configuration.getClass().toString());
    }

    private AbstractModbusMaster createClient(ModbusTransportConfiguration configuration) {
        if (configuration instanceof ModbusTcpTransportConfiguration) {
            ModbusTcpTransportConfiguration tcpConf = (ModbusTcpTransportConfiguration) configuration;
            log.trace("Creating TCP Modbus client [{}]", tcpConf);
            return new ModbusTCPMaster(tcpConf.getHost(),
                    tcpConf.getPort(),
                    tcpConf.getTimeout(),
                    tcpConf.isReconnect(),
                    tcpConf.isRtuOverTcp());
        } else if (configuration instanceof ModbusUdpTransportConfiguration) {
            ModbusUdpTransportConfiguration udpConf = (ModbusUdpTransportConfiguration) configuration;
            log.trace("Creating UDP Modbus client [{}]", udpConf);
            return new ModbusUDPMaster(udpConf.getHost(),
                    udpConf.getPort(),
                    udpConf.getTimeout());
        } else if (configuration instanceof ModbusRtuTransportConfiguration) {
            ModbusRtuTransportConfiguration rtuConf = (ModbusRtuTransportConfiguration) configuration;
            SerialParameters serialParams = new SerialParameters();
            serialParams.setPortName(rtuConf.getPortName());
            serialParams.setBaudRate(rtuConf.getBaudRate());
            serialParams.setDatabits(Integer.toString(rtuConf.getDataBits())); // cast to string due to range validation while parsing
            serialParams.setStopbits(Float.toString(rtuConf.getStopBits())); // cast to string due to range validation while parsing
            serialParams.setParity(rtuConf.getParity());
            serialParams.setEncoding(rtuConf.getEncoding());

            log.trace("Creating RTU Modbus client [{}]", rtuConf);
            return new ModbusSerialMaster(serialParams, rtuConf.getTimeout());
        } else {
            log.error("[{}] Unknown Modbus transport configuration {}", gateway.getTenantLabel(), configuration);
            throw new IllegalArgumentException("Unknown Modbus transport configuration");
        }
    }

    public void connect() {
        cachedThreadPool.execute(this::checkConnection);
    }

    private void checkConnection() {
        if (null == client.getTransport()) {
            synchronized (connectLock) {
                while (null == client.getTransport() && !executor.isShutdown()) {
                    log.trace("MBS[{}] connecting...", serverName);
                    try {
                        client.connect();
                        transaction = client.getTransport().createTransaction();
                        log.info("MBS[{}] connected", serverName);

                        firstRead();
                        startPolling();
                    } catch (Exception e) {
                        log.error("MBS[{}] connection failed!", serverName, e);
                        if (null == client.getTransport()) {
                            try {
                                Thread.sleep(configuration.getTransport().getRetryInterval());
                            } catch (InterruptedException e1) {
                                log.trace("Failed to wait for retry interval!", e);
                            }
                        }
                    }
                }
            }
        }
    }

    private void firstRead() {
        log.info("MBS[{}] going to first read", serverName);

        devices.forEach((name, device) -> {
            readTags(device, device.getAttributesMappings());
            readTags(device, device.getTimeseriesMappings());

            gateway.onDeviceConnect(device.getName(), null);
            gateway.subscribe(new RpcCommandSubscription(device.getName(), rpcProcessor));

            log.info("MBS[{}] connected new MBD[{}]", serverName, device.getName());

            checkDeviceDataUpdates(device);
        });
    }

    private void startPolling() {
        log.info("Start polling MBS[{}]", serverName);

        devices.forEach((name, device) -> {
            device.getSortedTagMappings().entrySet().stream().forEach(tags -> {
                log.info("MBS[{}] Schedule polling [{}] slave for tag(s) [{}], period {} ms",
                        serverName,
                        device.getName(),
                        tags.getValue().stream().map(t -> t.getTag()).collect(Collectors.joining(",")),
                        tags.getKey());

                executor.scheduleAtFixedRate(()->{
                            device.clearUpdates();
                            readTags(device, tags.getValue());
                            checkDeviceDataUpdates(device);
                        },
                        tags.getKey(), tags.getKey(), TimeUnit.MILLISECONDS);
            });
        });
    }

    private void checkDeviceDataUpdates(ModbusDevice device) {
        if (!device.getAttributesUpdate().isEmpty()) {
            gateway.onDeviceAttributesUpdate(device.getName(), device.getAttributesUpdate());
        }
        if (!device.getTimeseriesUpdate().isEmpty()) {
            gateway.onDeviceTelemetry(device.getName(), device.getTimeseriesUpdate());
        }
    }

    private void readTags(ModbusDevice device, List<PollingTagMapping> mappings) {
        mappings.stream().forEach(m -> readTag(device, m));
    }

    private void readTag(ModbusDevice device, PollingTagMapping mapping) {
        ModbusRequest request = createRequest(device, mapping);
        transaction.setRequest(request);
        try {
            transaction.execute();
            processResponse(transaction.getResponse(), device, mapping);
        } catch (ModbusException e) {
            log.error("MBS[{}] failed to read from MBD[{}], tag [{}]", serverName, device.getName(), mapping.getTag(), e);
        }
    }

    private ModbusRequest createRequest(ModbusDevice device, PollingTagMapping mapping) {
        ModbusRequest request = null;

        switch (mapping.getFunctionCode()) {
            case Modbus.READ_COILS:
                request = new ReadCoilsRequest(mapping.getAddress(), ModbusExtensionConstants.DEFAULT_REGISTER_COUNT_FOR_BOOLEAN);
                break;
            case Modbus.READ_INPUT_DISCRETES:
                request = new ReadInputDiscretesRequest(mapping.getAddress(), ModbusExtensionConstants.DEFAULT_REGISTER_COUNT_FOR_BOOLEAN);
                break;
            case Modbus.READ_INPUT_REGISTERS:
                request = new ReadInputRegistersRequest(mapping.getAddress(), mapping.getRegisterCount());
                break;
            case Modbus.READ_HOLDING_REGISTERS:
                request = new ReadMultipleRegistersRequest(mapping.getAddress(), mapping.getRegisterCount());
                break;
            default:
                log.error("MBS[{}] function {} is not supported: MBD[{}], tag [{}]",
                        serverName, mapping.getFunctionCode(), device.getName(), mapping.getTag());
                throw new IllegalArgumentException("Unsupported Modbus function " + mapping.getFunctionCode());
        }

        request.setUnitID(device.getUnitId());

        return request;
    }

    private void processResponse(ModbusResponse response, ModbusDevice device, PollingTagMapping mapping) {
        switch (mapping.getFunctionCode()) {
            case Modbus.READ_COILS:
                ReadCoilsResponse rcResp = (ReadCoilsResponse) response;
                device.updateTag(mapping, rcResp.getCoils());
                break;
            case Modbus.READ_INPUT_DISCRETES:
                ReadInputDiscretesResponse ridResp = (ReadInputDiscretesResponse) response;
                device.updateTag(mapping, ridResp.getDiscretes());
                break;
            case Modbus.READ_INPUT_REGISTERS:
                ReadInputRegistersResponse rirResp = (ReadInputRegistersResponse) response;
                device.updateTag(mapping, rirResp.getRegisters());
                break;
            case Modbus.READ_HOLDING_REGISTERS:
                ReadMultipleRegistersResponse rmrResp = (ReadMultipleRegistersResponse) response;
                device.updateTag(mapping, rmrResp.getRegisters());
                break;
            default:
                log.error("MBS[{}] function {} is not supported: MBD[{}], tag [{}]",
                        serverName, mapping.getFunctionCode(), device.getName(), mapping.getTag());
                throw new IllegalArgumentException("Unsupported Modbus function " + mapping.getFunctionCode());
        }
    }

    public void disconnect() {
        executor.shutdownNow();

        log.trace("Disconnecting from MBS[{}]", serverName);
        client.disconnect();
        log.info("Disconnected from MBS[{}]", serverName);

        devices.forEach((name, device)->{
            gateway.onDeviceDisconnect(device.getName());
            log.info("MBS[{}] disconnected MBD[{}]", serverName, name);
        });
    }
}