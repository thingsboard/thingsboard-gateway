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
package org.thingsboard.gateway;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Rule;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Valerii Sosliuk on 5/9/2018.
 */
@Slf4j
public class AbstractGatewayIntegrationTest {

    @After
    public void shutdownRules() {
        Set<Field> set = new HashSet<>();
        Class<?> c = this.getClass();
        while (c != null) {
            Arrays.stream(c.getDeclaredFields()).filter(f -> f.isAnnotationPresent(Rule.class)).forEach(this::shutDownIfApplicable);
            c = c.getSuperclass();
        }
    }

    private void shutDownIfApplicable(Field field) {
        try {
            Object rule = field.get(this);
            Arrays.stream(rule.getClass().getDeclaredMethods()).filter(m -> m.isAnnotationPresent(After.class))
                    .forEach(m -> {
                        try {
                            m.invoke(rule, (Object[])null);
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            log.warn("Failed to invoke method [" + m.getName() + "]. Reason: " + e.getMessage(), e);
                        }
                    });
        } catch (IllegalAccessException e) {
            log.warn(e.getMessage(), e);
        }
    }
}
