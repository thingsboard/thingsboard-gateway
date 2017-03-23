package org.thingsboard.gateway.extensions.sigfox.conf.mapping;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.thingsboard.gateway.extensions.opc.conf.mapping.DeviceMapping;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.server.common.data.kv.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Data
@Slf4j
public class SigfoxDeviceMapping {

    public static final Pattern TAG_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");

    private String deviceNameJsonExpression;
    private final List<AttributesMapping> attributes;
    private final List<TimeseriesMapping> timeseries;

    public DeviceData parseBody(String body) {
        try {
            DocumentContext document = JsonPath.parse(body);
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
            throw e;
        }
        return null;
    }

    private List<KvEntry> getKvEntries(DocumentContext document, List<? extends SigfoxKVMapping> mappings) {
        List<KvEntry> result = new ArrayList<>();
        if (mappings != null) {
            for (SigfoxKVMapping mapping : mappings) {
                String key = eval(document, mapping.getKey());
                String strVal = eval(document, mapping.getValue());
                switch (mapping.getType().getDataType()) {
                    case STRING:
                        result.add(new StringDataEntry(key,
                                applyTransformer((DataValueTransformer<String>) mapping.getTransformer(), strVal)));
                        break;
                    case BOOLEAN:
                        result.add(new BooleanDataEntry(key,
                                applyTransformer((DataValueTransformer<Boolean>) mapping.getTransformer(), Boolean.valueOf(strVal))));
                        break;
                    case DOUBLE:
                        result.add(new DoubleDataEntry(key,
                                applyTransformer((DataValueTransformer<Double>) mapping.getTransformer(), Double.valueOf(strVal))));
                        break;
                    case LONG:
                        result.add(new LongDataEntry(key,
                                applyTransformer((DataValueTransformer<Long>) mapping.getTransformer(), Long.valueOf(strVal))));
                        break;
                }
            }
        }
        return result;
    }

    private static <T> T applyTransformer(DataValueTransformer<T> transformer, T value) {
        if (transformer != null) {
            try {
                return transformer.transform(value);
            } catch (Exception e) {
                log.error("Transformer [{}] can't be applied to field with key [{}] and value [{}]",
                        transformer.getName(), value);
                throw e;
            }
        } else {
            return value;
        }
    }

    protected static String eval(DocumentContext document, String expression) {
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

    protected static <T> T apply(DocumentContext document, String expression) {
        try {
            return document.read(expression);
        } catch (RuntimeException e) {
            log.debug("Failed to apply expression: {}", expression, e);
            throw new RuntimeException("Failed to apply expression " + expression);
        }
    }
}
