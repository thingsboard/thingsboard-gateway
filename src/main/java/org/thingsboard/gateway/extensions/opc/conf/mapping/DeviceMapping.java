package org.thingsboard.gateway.extensions.opc.conf.mapping;

import lombok.Data;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ashvayka on 16.01.17.
 */
@Data
public class DeviceMapping {

    private final String deviceNodePattern;
    private final String deviceIdPattern;
    private final List<AttributesMapping> attributes;
    private final List<TimeseriesMapping> timeseries;

    public Set<String> getTags() {
        Set<String> tags = new HashSet<>();
        Pattern pattern = Pattern.compile("\\$\\{(.*?)\\}");
        addTags(tags, pattern, deviceNodePattern);
        addTags(tags, pattern, deviceIdPattern);
        attributes.forEach(mapping -> addTags(tags, pattern, mapping.getValue()));
        timeseries.forEach(mapping -> addTags(tags, pattern, mapping.getValue()));
        return tags;
    }

    public void addTags(Set<String> tags, Pattern pattern, String expression) {
        Matcher matcher = pattern.matcher(expression);
        while (matcher.find()) {
            String tag = matcher.group();
            tags.add(tag.substring(2, tag.length() - 1));
        }
    }
}
