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

package org.thingsboard.gateway.extensions.modbus.conf.mapping;

import lombok.Data;
import org.thingsboard.gateway.extensions.common.conf.mapping.DataTypeMapping;
import org.thingsboard.gateway.extensions.modbus.conf.ModbusExtensionConstants;

@Data
public class TagMapping {
    private String tag;
    private int functionCode;
    private int address;
    private int registerCount = ModbusExtensionConstants.DEFAULT_REGISTER_COUNT;
    private Boolean coilStatus = ModbusExtensionConstants.DEFAULT_COIL_STATUS;
    private String byteOrder = ModbusExtensionConstants.BIG_ENDIAN_BYTE_ORDER;
    private int bit = ModbusExtensionConstants.NO_BIT_INDEX_DEFINED;
}
