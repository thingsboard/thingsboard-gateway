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

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.thingsboard.gateway.extensions.sigfox.conf.mapping.AttributesMapping;
import org.thingsboard.gateway.extensions.sigfox.conf.mapping.DataValueTransformer;
import org.thingsboard.gateway.extensions.sigfox.conf.mapping.SigfoxKVMapping;
import org.thingsboard.gateway.extensions.sigfox.conf.mapping.TimeseriesMapping;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.util.converter.AbstractJsonConverter;
import org.thingsboard.gateway.util.converter.BasicJsonConverter;
import org.thingsboard.server.common.data.kv.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class CsvDeviceDataConverter extends BasicJsonConverter {

}
