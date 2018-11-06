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
package org.thingsboard.gateway.util.converter;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.util.converter.transformer.DataValueTransformer;
import org.thingsboard.server.common.data.kv.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ashvayka on 15.05.17.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class BasicJsonConverter extends AbstractJsonConverter {

    protected String filterExpression;
    protected String deviceNameJsonExpression;
    protected String deviceTypeJsonExpression;
    protected List<AttributesMapping> attributes;
    protected List<TimeseriesMapping> timeseries;
    private ConcurrentHashMap<String, SimpleDateFormat> formatters = new ConcurrentHashMap<>();

    public DeviceData parseBody(String body) {
        try {
            return parseDeviceData(JsonPath.parse(body));
        } catch (Exception e) {
            log.error("Exception occurred while parsing json request body [{}]", body, e);
            throw new RuntimeException(e);
        }
    }

    protected DeviceData parseDeviceData(DocumentContext document) throws ParseException {
        long ts = System.currentTimeMillis();
        String deviceName = eval(document, deviceNameJsonExpression);
        String deviceType = null;
        if (!StringUtils.isEmpty(deviceTypeJsonExpression)) {
            deviceType = eval(document, deviceTypeJsonExpression);
        }
        if (!StringUtils.isEmpty(deviceName)) {
            List<KvEntry> attrData = getKvEntries(document, attributes);
            List<TsKvEntry> tsData = getTsKvEntries(document, timeseries, ts);
            return new DeviceData(deviceName, deviceType, attrData, tsData);
        } else {
            return null;
        }
    }

    protected List<TsKvEntry> getTsKvEntries(DocumentContext document, List<? extends TimeseriesMapping> mappings, long defaultTs) throws ParseException {
        List<TsKvEntry> result = new ArrayList<>();
        if (mappings != null) {
            for (TransformerKVMapping mapping : mappings) {
                String key = eval(document, mapping.getKey());
                String strVal = eval(document, mapping.getValue());
                if(mapping.getValue().equals(strVal)){
                    //取值错误，跳过这个数据
                    continue;
                }
                long ts = defaultTs;
                if (!StringUtils.isEmpty(mapping.getTs())) {
                    String tsVal = eval(document, mapping.getTs());
                    if (!StringUtils.isEmpty(mapping.getTsFormat())) {
                        SimpleDateFormat formatter = formatters.computeIfAbsent(mapping.getTsFormat(), SimpleDateFormat::new);
                        ts = formatter.parse(tsVal).getTime();
                    } else {
                        ts = Long.parseLong(tsVal);
                    }
                }
                DataValueTransformer transformer = mapping.getTransformer();
                if (transformer != null && transformer.isApplicable(strVal)) {
                    result.add(new BasicTsKvEntry(ts, getKvEntry(mapping, key, strVal, transformer)));
                } else if (transformer == null) {
                    result.add(new BasicTsKvEntry(ts, getKvEntry(mapping, key, strVal)));
                }
            }
        }
        return result;
    }

    protected List<KvEntry> getKvEntries(DocumentContext document, List<? extends TransformerKVMapping> mappings) {
        List<KvEntry> result = new ArrayList<>();
        if (mappings != null) {
            for (TransformerKVMapping mapping : mappings) {
                String key = eval(document, mapping.getKey());
                String strVal = eval(document, mapping.getValue());
                if(mapping.getValue().equals(strVal)){
                    //取值错误，跳过这个数据
                    continue;
                }
                DataValueTransformer transformer = mapping.getTransformer();
                if (transformer != null && transformer.isApplicable(strVal)) {
                    result.add(getKvEntry(mapping, key, strVal, transformer));
                } else if (transformer == null) {
                    KvEntry kvEntry = getKvEntry(mapping, key, strVal);
                    if (kvEntry != null) {
                        result.add(getKvEntry(mapping, key, strVal));
                    }
                }
            }
        }
        return result;
    }

    private BasicKvEntry getKvEntry(TransformerKVMapping mapping, String key, String strVal, DataValueTransformer transformer) {
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
                    transformer.getClass().getSimpleName(), key, strVal);
            throw e;
        }
        log.error("No mapping found for data type [{}]", mapping.getType().getDataType());
        throw new IllegalArgumentException("No mapping found for data type [" + mapping.getType().getDataType() + "]");
    }

    private BasicKvEntry getKvEntry(TransformerKVMapping mapping, String key, String strVal) {
        if (strVal == null) {
            return null;
        }
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
        log.error("No mapping found for data type [{}]", mapping.getType().getDataType());
        throw new IllegalArgumentException("No mapping found for data type [" + mapping.getType().getDataType() + "]");
    }

}
