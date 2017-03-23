package org.thingsboard.gateway.extensions.sigfox.conf.mapping;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
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
