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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.thingsboard.gateway.util.converter.transformer.thingPark.ThingParkTransformer;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DoubleValueTransformer.class, name = DoubleValueTransformer.INT_TO_DOUBLE_TRANSFORMER_NAME),
        @JsonSubTypes.Type(value = ThingParkTransformer.class, name = ThingParkTransformer.THING_PARK_CONVERTER_NAME)
})
public interface DataValueTransformer {

    Double transformToDouble(String strValue);

    Long transformToLong(String strValue);

    String transformToString(String strValue);

    Boolean transformToBoolean(String strValue);

    boolean isApplicable(String strValue);

}