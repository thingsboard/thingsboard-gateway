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
package org.thingsboard.gateway.extensions.sigfox.conf;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.util.converter.AttributesMapping;
import org.thingsboard.gateway.util.converter.BasicJsonConverter;
import org.thingsboard.gateway.util.converter.TimeseriesMapping;

import java.util.List;
import java.util.regex.Pattern;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class SigfoxDeviceDataConverter extends BasicJsonConverter {

    public static final Pattern TAG_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");

    private String filterExpression;
    private String deviceNameJsonExpression;
    private String deviceTypeJsonExpression;
    private final List<AttributesMapping> attributes;
    private final List<TimeseriesMapping> timeseries;

    public boolean isApplicable(String body) {
        if (filterExpression == null || filterExpression.isEmpty()) {
            return true;
        } else {
            try {
                List jsonArray = JsonPath.parse(body).read(filterExpression);
                return !jsonArray.isEmpty();
            } catch (RuntimeException e) {
                log.debug("Failed to apply filter expression: {}", filterExpression, e);
                throw new RuntimeException("Failed to apply filter expression " + filterExpression, e);
            }
        }
    }

    public DeviceData parseBody(String body) {
        try {
            if (filterExpression != null && !filterExpression.isEmpty()) {
                DocumentContext document = JsonPath.parse(body);
                try {
                    log.debug("Data before filtering {}", body);
                    List jsonArray = document.read(filterExpression);
                    Object jsonObj = jsonArray.get(0); // take 1st element from filtered array (jayway jsonpath library limitation)
                    document = JsonPath.parse(jsonObj);
                    body = document.jsonString();
                    log.debug("Data after filtering {}", body);
                } catch (RuntimeException e) {
                    log.debug("Failed to apply filter expression: {}", filterExpression, e);
                    throw new RuntimeException("Failed to apply filter expression " + filterExpression, e);
                }
            }

            return parseDeviceData(JsonPath.parse(body));
        } catch (Exception e) {
            log.error("Exception occurred while parsing json request body [{}]", body, e);
            throw new RuntimeException("Exception occurred while parsing json request body [" + body + "]", e);
        }
    }
}
