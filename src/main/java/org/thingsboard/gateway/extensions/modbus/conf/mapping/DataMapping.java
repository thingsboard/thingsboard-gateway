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

@Data
public class DataMapping {
    private String key;
    private DataTypeMapping type;
    private String functionCode;
    private int address; //TODO: If we need hexadecimal value, the String type need to be used as it does for the function code.
    private Boolean bigEndian = true;
    private int registerCount = 1;
}
