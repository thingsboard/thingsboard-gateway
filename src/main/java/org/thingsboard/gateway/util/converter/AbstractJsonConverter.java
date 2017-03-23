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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.gateway.extensions.opc.conf.mapping.DeviceMapping;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ashvayka on 02.03.17.
 */
@Slf4j
public abstract class AbstractJsonConverter {

    protected static final ObjectMapper mapper = new ObjectMapper();

    protected static String eval(String topic, Pattern pattern, DocumentContext document, String expression) {
        if (pattern != null) {
            return eval(topic, pattern);
        } else {
            return eval(document, expression);
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

    protected static Pattern checkAndCompile(Pattern deviceNameTopicPattern, String deviceNameTopicExpression) {
        if (deviceNameTopicPattern != null) {
            return deviceNameTopicPattern;
        } else if (deviceNameTopicExpression != null) {
            return Pattern.compile(deviceNameTopicExpression);
        } else {
            return null;
        }
    }

    protected static String eval(String topic, Pattern pattern) {
        Matcher matcher = pattern.matcher(topic);
        while (matcher.find()) {
            return matcher.group();
        }
        return null;
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
