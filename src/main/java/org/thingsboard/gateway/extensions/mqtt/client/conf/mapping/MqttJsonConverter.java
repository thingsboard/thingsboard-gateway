/**
 * Copyright © ${project.inceptionYear}-2017 The Thingsboard Authors
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.thingsboard.gateway.extensions.opc.conf.mapping.AttributesMapping;
import org.thingsboard.gateway.extensions.opc.conf.mapping.DeviceMapping;
import org.thingsboard.gateway.extensions.opc.conf.mapping.KVMapping;
import org.thingsboard.gateway.extensions.opc.conf.mapping.TimeseriesMapping;
import org.thingsboard.gateway.service.DeviceData;
import org.thingsboard.server.common.data.kv.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * Created by ashvayka on 23.01.17.
 */
@Data
@Slf4j
public class MqttJsonConverter implements MqttDataConverter {

    private static final ObjectMapper mapper = new ObjectMapper();
    private String filterExpression;
    private String deviceNameExpression;
    private final List<AttributesMapping> attributes;
    private final List<TimeseriesMapping> timeseries;

    @Override
    public List<DeviceData> convert(MqttMessage msg) throws Exception {
        String data = new String(msg.getPayload(), StandardCharsets.UTF_8);
        log.trace("Parsing json message: {}", data);
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

        return parse(srcList);
    }

    private List<DeviceData> parse(List<String> srcList) {
        List<DeviceData> result = new ArrayList<>(srcList.size());
        for (String src : srcList) {
            DocumentContext document = JsonPath.parse(src);
            if (!filterExpression.isEmpty()) {
                try {
                    document = JsonPath.parse((Object) document.read(filterExpression));
                } catch (RuntimeException e) {
                    log.debug("Failed to apply filter expression: {}", filterExpression);
                    throw new RuntimeException("Failed to apply filter expression " + filterExpression);
                }
            }

            long ts = System.currentTimeMillis();
            String deviceName = eval(document, deviceNameExpression);
            List<KvEntry> attrData = getKvEntries(document, attributes);
            List<TsKvEntry> tsData = getKvEntries(document, timeseries).stream()
                    .map(kv -> new BasicTsKvEntry(ts, kv))
                    .collect(Collectors.toList());
            result.add(new DeviceData(deviceName, attrData, tsData));
        }
        return result;
    }

    private List<KvEntry> getKvEntries(DocumentContext document, List<? extends KVMapping> mappings) {
        List<KvEntry> result = new ArrayList<>();
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
        return result;
    }


    private String eval(DocumentContext document, String expression) {
        Matcher matcher = DeviceMapping.TAG_PATTERN.matcher(expression);
        String result = new String(expression);
        while (matcher.find()) {
            String tag = matcher.group();
            String exp = tag.substring(2, tag.length() - 1);
            String tagValue = ((Object) apply(document, exp)).toString();
            result = result.replace(tag, tagValue);
        }
        return result;
    }

    private <T> T apply(DocumentContext document, String expression) {
        try {
            return document.read(expression);
        } catch (RuntimeException e) {
            log.debug("Failed to apply expression: {}", expression);
            throw new RuntimeException("Failed to apply expression " + expression);
        }
    }
}
