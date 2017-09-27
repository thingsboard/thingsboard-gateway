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
package org.thingsboard.gateway.util.converter.transformer.thingPark;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Setter;
import org.bouncycastle.util.encoders.Hex;
import org.thingsboard.gateway.util.converter.transformer.AbstractDataValueTransformer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ThingParkTransformer extends AbstractDataValueTransformer {

    public static final String THING_PARK_CONVERTER_NAME = "thingParkTransformer";

    @Setter
    private String keyValue;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Double transformToDouble(String strValue) {

        byte[] bytes = Hex.decode(strValue);
        String jsonInput;
        try {
            jsonInput = new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        TypeReference<HashMap<String, String>> typeRef
                = new TypeReference<HashMap<String, String>>() {};
        Map<String, String> map;

        try {
            map = mapper.readValue(jsonInput, typeRef);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Double returnValue;
        if (Objects.equals(keyValue, "T")) {
            returnValue = Double.parseDouble(map.get("T"));
        }else if (Objects.equals(keyValue, "H")) {
            returnValue = Double.parseDouble(map.get("H"));
        }else{
            throw new IllegalArgumentException("Not found such keyValue [" + keyValue + "]");
        }

        return returnValue;
    }
}
