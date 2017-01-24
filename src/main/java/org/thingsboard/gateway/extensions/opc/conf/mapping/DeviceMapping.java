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

    public static final Pattern TAG_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");
    private final String deviceNodePattern;
    private final String deviceNamePattern;
    private final List<AttributesMapping> attributes;
    private final List<TimeseriesMapping> timeseries;

    public Set<String> getDeviceNameTags() {
        Set<String> tags = new HashSet<>();
        addTags(tags, TAG_PATTERN, deviceNamePattern);
        return tags;
    }

    public Set<String> getAllTags() {
        Set<String> tags = new HashSet<>();
        addTags(tags, TAG_PATTERN, deviceNamePattern);
        attributes.forEach(mapping -> addTags(tags, TAG_PATTERN, mapping.getValue()));
        timeseries.forEach(mapping -> addTags(tags, TAG_PATTERN, mapping.getValue()));
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
