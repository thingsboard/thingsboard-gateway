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
package org.thingsboard.gateway.extensions.mqtt.client.conf.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.util.StringUtils;
import org.thingsboard.gateway.util.converter.AbstractJsonConverter;
import org.thingsboard.gateway.extensions.opc.conf.mapping.AttributesMapping;
import org.thingsboard.gateway.extensions.common.conf.mapping.KVMapping;
import org.thingsboard.gateway.extensions.opc.conf.mapping.TimeseriesMapping;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.server.common.data.kv.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by ashvayka on 23.01.17.
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Slf4j
public class MqttJsonConverter extends AbstractJsonConverter implements MqttDataConverter {

    private String filterExpression;
    private String deviceNameJsonExpression;
    private String deviceNameTopicExpression;
    private Pattern deviceNameTopicPattern;

    private String deviceTypeJsonExpression;
    private String deviceTypeTopicExpression;
    private Pattern deviceTypeTopicPattern;
    private int timeout;
    private final List<AttributesMapping> attributes;
    private final List<TimeseriesMapping> timeseries;

    @Override
    public List<DeviceData> convert(String topic, MqttMessage msg) throws Exception {
        String data = new String(msg.getPayload(), StandardCharsets.UTF_8);
        log.trace("Parsing json message: {}", data);

        if (!filterExpression.isEmpty()) {
            try {
                log.debug("Data before filtering {}", data);
                DocumentContext document = JsonPath.parse(data);
                document = JsonPath.parse((Object) document.read(filterExpression));
                data = document.jsonString();
                log.debug("Data after filtering {}", data);
            } catch (RuntimeException e) {
                log.debug("Failed to apply filter expression: {}", filterExpression);
                throw new RuntimeException("Failed to apply filter expression " + filterExpression);
            }
        }

        JsonNode node = mapper.readTree(data);
        List<String> srcList;
        if (node.isArray()) {
            srcList = new ArrayList<>(node.size());
            for (int i = 0; i < node.size(); i++) {
                srcList.add(mapper.writeValueAsString(node.get(i)));
            }
        } else {
            srcList = Collections.singletonList(data);
        }

        return parse(topic, srcList);
    }

    private List<DeviceData> parse(String topic, List<String> srcList) {
        List<DeviceData> result = new ArrayList<>(srcList.size());
        for (String src : srcList) {
            DocumentContext document = JsonPath.parse(src);
            long ts = System.currentTimeMillis();
            String deviceName;
            String deviceType = null;
            if (!StringUtils.isEmpty(deviceNameTopicExpression)) {
                deviceName = evalDeviceName(topic);
            } else {
                deviceName = eval(document, deviceNameJsonExpression);
            }
            if (!StringUtils.isEmpty(deviceTypeTopicExpression)) {
                deviceType = evalDeviceType(topic);
            } else if (!StringUtils.isEmpty(deviceTypeJsonExpression)) {
                deviceType = eval(document, deviceTypeJsonExpression);
            }

            if (!StringUtils.isEmpty(deviceName)) {
                List<KvEntry> attrData = getKvEntries(document, attributes);
                List<TsKvEntry> tsData = getKvEntries(document, timeseries).stream()
                        .map(kv -> new BasicTsKvEntry(ts, kv))
                        .collect(Collectors.toList());
                result.add(new DeviceData(deviceName, deviceType, attrData, tsData, timeout));
            }
        }
        return result;
    }

    private List<KvEntry> getKvEntries(DocumentContext document, List<? extends KVMapping> mappings) {
        List<KvEntry> result = new ArrayList<>();
        if (mappings != null) {
            for (KVMapping mapping : mappings) {
                String key = eval(document, mapping.getKey());
                String strVal = eval(document, mapping.getValue());
                switch (mapping.getType().getDataType()) {
                    case STRING:
                        result.add(new StringDataEntry(key, strVal));
                        break;
                    case BOOLEAN:
                        result.add(new BooleanDataEntry(key, Boolean.valueOf(strVal)));
                        break;
                    case DOUBLE:
                        result.add(new DoubleDataEntry(key, Double.valueOf(strVal)));
                        break;
                    case LONG:
                        result.add(new LongDataEntry(key, Long.valueOf(strVal)));
                        break;
                }
            }
        }
        return result;
    }

    private String evalDeviceName(String topic) {
        if (deviceNameTopicPattern == null) {
            deviceNameTopicPattern = Pattern.compile(deviceNameTopicExpression);
        }
        Matcher matcher = deviceNameTopicPattern.matcher(topic);
        while (matcher.find()) {
            return matcher.group();
        }
        return null;
    }

    private String evalDeviceType(String topic) {
        if (deviceTypeTopicPattern == null) {
            deviceTypeTopicPattern = Pattern.compile(deviceTypeJsonExpression);
        }
        Matcher matcher = deviceTypeTopicPattern.matcher(topic);
        while (matcher.find()) {
            return matcher.group();
        }
        return null;
    }

}
