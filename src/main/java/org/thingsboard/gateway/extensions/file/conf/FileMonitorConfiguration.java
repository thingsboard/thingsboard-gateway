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
package org.thingsboard.gateway.extensions.file.conf;

import lombok.Data;

/**
 * Created by ashvayka on 15.05.17.
 */
@Data
public class FileMonitorConfiguration {

    private String file;
    private int skipLines;
    private int updateInterval;
    private String[] csvColumns;
    private CsvDeviceDataConverter converter;
}
