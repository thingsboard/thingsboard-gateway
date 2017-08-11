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
package org.thingsboard.gateway.util.converter.transformer;


public class DoubleValueTransformer implements DataValueTransformer {

    static final String INT_TO_DOUBLE_TRANSFORMER_NAME = "intToDouble";

    private static final int MAX_DOUBLE_VALUE = 65536;
    private static final int DIVIDE_POWER = 10;

    @Override
    public Double transformToDouble(String strValue) {
        Double value = Double.valueOf(strValue);
        if (value <= MAX_DOUBLE_VALUE) {
            return value / DIVIDE_POWER;
        } else {
            return (MAX_DOUBLE_VALUE - value) / DIVIDE_POWER;
        }
    }

    @Override
    public Long transformToLong(String strValue) {
        return Long.valueOf(strValue);
    }

    @Override
    public String transformToString(String strValue) {
        return strValue;
    }

    @Override
    public Boolean transformToBoolean(String strValue) {
        return Boolean.valueOf(strValue);
    }

    @Override
    public String getName() {
        return INT_TO_DOUBLE_TRANSFORMER_NAME;
    }
}

