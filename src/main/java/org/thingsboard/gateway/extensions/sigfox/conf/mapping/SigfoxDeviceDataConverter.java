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
package org.thingsboard.gateway.extensions.sigfox.conf.mapping;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.thingsboard.gateway.extensions.sigfox.conf.mapping.transformer.DataValueTransformer;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.util.converter.AbstractJsonConverter;
import org.thingsboard.server.common.data.kv.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class SigfoxDeviceDataConverter extends AbstractJsonConverter {

    public static final Pattern TAG_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");

    private String filterExpression;
    private String deviceNameJsonExpression;
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
            DocumentContext document = JsonPath.parse(body);

            if (filterExpression != null && !filterExpression.isEmpty()) {
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

            long ts = System.currentTimeMillis();
            String deviceName = eval(document, deviceNameJsonExpression);
            if (!StringUtils.isEmpty(deviceName)) {
                List<KvEntry> attrData = getKvEntries(JsonPath.parse(body), attributes);
                List<TsKvEntry> tsData = getKvEntries(JsonPath.parse(body), timeseries).stream()
                        .map(kv -> new BasicTsKvEntry(ts, kv))
                        .collect(Collectors.toList());
                return new DeviceData(deviceName, attrData, tsData);
            }
        } catch (Exception e) {
            log.error("Exception occurred while parsing json request body [{}]", body, e);
            throw new RuntimeException("Exception occurred while parsing json request body [" + body +"]", e);
        }
        return null;
    }

    private List<KvEntry> getKvEntries(DocumentContext document, List<? extends SigfoxKVMapping> mappings) {
        List<KvEntry> result = new ArrayList<>();
        if (mappings != null) {
            for (SigfoxKVMapping mapping : mappings) {
                String key = eval(document, mapping.getKey());
                String strVal = eval(document, mapping.getValue());
                result.add(getKvEntry(mapping, key, strVal));
            }
        }
        return result;
    }

    private BasicKvEntry getKvEntry(SigfoxKVMapping mapping, String key, String strVal) {
        DataValueTransformer transformer = mapping.getTransformer();
        if (transformer != null) {
            try {
                switch (mapping.getType().getDataType()) {
                    case STRING:
                        return new StringDataEntry(key, transformer.transformToString(strVal));
                    case BOOLEAN:
                        return new BooleanDataEntry(key, transformer.transformToBoolean(strVal));
                    case DOUBLE:
                        return new DoubleDataEntry(key, transformer.transformToDouble(strVal));
                    case LONG:
                        return new LongDataEntry(key, transformer.transformToLong(strVal));
                }
            } catch (Exception e) {
                log.error("Transformer [{}] can't be applied to field with key [{}] and value [{}]",
                        transformer.getName(), key, strVal);
                throw e;
            }
        } else {
            switch (mapping.getType().getDataType()) {
                case STRING:
                    return new StringDataEntry(key, strVal);
                case BOOLEAN:
                    return new BooleanDataEntry(key, Boolean.valueOf(strVal));
                case DOUBLE:
                    return new DoubleDataEntry(key, Double.valueOf(strVal));
                case LONG:
                    return new LongDataEntry(key, Long.valueOf(strVal));
            }
        }

        log.error("No mapping found for data type [{}]", mapping.getType().getDataType());
        throw new IllegalArgumentException("No mapping found for data type [" + mapping.getType().getDataType() + "]");
    }
}
