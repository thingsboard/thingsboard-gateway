/**
 * Copyright © ${project.inceptionYear}-2017 The Thingsboard Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.gateway.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.thingsboard.server.common.data.kv.KvEntry;

import java.nio.charset.StandardCharsets;

/**
 * Created by ashvayka on 19.01.17.
 */
public class JsonTools {

    private static final ObjectMapper JSON = new ObjectMapper();

    public static ObjectNode newNode() {
        return JSON.createObjectNode();
    }

    public static byte[] toBytes(ObjectNode node) {
        return toString(node).getBytes(StandardCharsets.UTF_8);
    }

    public static String toString(ObjectNode node) {
        try {
            return JSON.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void putToNode(ObjectNode node, KvEntry kv) {
        switch (kv.getDataType()) {
            case BOOLEAN:
                node.put(kv.getKey(), kv.getBooleanValue().get());
                break;
            case STRING:
                node.put(kv.getKey(), kv.getStrValue().get());
                break;
            case LONG:
                node.put(kv.getKey(), kv.getLongValue().get());
                break;
            case DOUBLE:
                node.put(kv.getKey(), kv.getDoubleValue().get());
                break;
        }
    }
}
