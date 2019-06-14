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

 package org.thingsboard.gateway.extensions.modbus.conf.transport;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Created by ashvayka on 16.01.17.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ModbusTcpTransportConfiguration.class, name = "tcp"),
        @JsonSubTypes.Type(value = ModbusUdpTransportConfiguration.class, name = "udp"),
        @JsonSubTypes.Type(value = ModbusRtuTransportConfiguration.class, name = "rtu")})
public interface ModbusTransportConfiguration {
    /**
     * 获取尝试周期时间
     *
     * @return
     */
    long getRetryInterval();
}