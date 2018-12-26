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
package org.thingsboard.gateway.extensions.opc.util;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UByte;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.ULong;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseDirection;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseResultMask;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseResult;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.eclipse.milo.opcua.stack.core.util.ConversionUtil.toList;

@Slf4j
public class OpcUaUtils {
    public static final String DATE_TIME_FORMAT = "yyyy-M-d H:m:s.SSS z";

    public static Map<String, NodeId> lookupTags(OpcUaClient client, NodeId nodeId, String deviceNodeName, Set<String> tags) {
        Map<String, NodeId> values = new HashMap<>();
        try {
            BrowseResult browseResult = client.browse(getBrowseDescription(nodeId)).get();
            List<ReferenceDescription> references = toList(browseResult.getReferences());

            for (ReferenceDescription rd : references) {
                NodeId childId;
                if (rd.getNodeId().isLocal()) {
                    childId = rd.getNodeId().local().get();
                } else {
                    log.trace("Ignoring remote node: {}", rd.getNodeId());
                    continue;
                }

                String browseName = rd.getBrowseName().getName();
                String name;
                String childIdStr = childId.getIdentifier().toString();
                if (childIdStr.contains(deviceNodeName)) {
                    name = childIdStr.substring(childIdStr.indexOf(deviceNodeName) + deviceNodeName.length() + 1, childIdStr.length());
                } else {
                    name = rd.getBrowseName().getName();
                }
                if (tags.contains(name)) {
                    values.put(name, childId);
                }
                // recursively browse to children
                values.putAll(lookupTags(client, childId, deviceNodeName, tags));
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Browsing nodeId={} failed: {}", nodeId, e.getMessage(), e);
        }
        return values;
    }

    public static BrowseDescription getBrowseDescription(NodeId nodeId) {
        return new BrowseDescription(
                nodeId,
                BrowseDirection.Forward,
                Identifiers.References,
                true,
                uint(NodeClass.Object.getValue() | NodeClass.Variable.getValue()),
                uint(BrowseResultMask.All.getValue())
        );
    }

    public static Variant convertToOpcValue(NodeId typeNode, Object value) {
        Number numberValue = (value instanceof Number) ? (Number) value : null;

        try {
            if (typeNode.equals(Identifiers.Double)) {
                return new Variant(Double.valueOf(numberValue.doubleValue()));
            } else if (typeNode.equals(Identifiers.Float)) {
                return new Variant(Float.valueOf(numberValue.floatValue()));
            } else if (typeNode.equals(Identifiers.String)) {
                return new Variant(value);
            } else if (typeNode.equals(Identifiers.Integer)|| typeNode.equals(Identifiers.Int32)) {
                return new Variant(Integer.valueOf(numberValue.intValue()));
            } else if (typeNode.equals(Identifiers.Int16)) {
                return new Variant(Short.valueOf(numberValue.shortValue()));
            } else if (typeNode.equals(Identifiers.Int64)) {
                return new Variant(Long.valueOf(numberValue.longValue()));
            } else if (typeNode.equals(Identifiers.UInteger) || typeNode.equals(Identifiers.UInt32)) {
                return new Variant(UInteger.valueOf(numberValue.intValue()));
            } else if (typeNode.equals(Identifiers.UInt16)) {
                return new Variant(UShort.valueOf(numberValue.shortValue()));
            } else if (typeNode.equals(Identifiers.UInt64)) {
                return new Variant(ULong.valueOf(numberValue.longValue()));
            } else if (typeNode.equals(Identifiers.Boolean)) {
                return new Variant(value);
            } else if (typeNode.equals(Identifiers.Byte)) {
                return new Variant(UByte.valueOf(numberValue.byteValue()));
            } else if (typeNode.equals(Identifiers.SByte)) {
                return new Variant(Byte.valueOf(numberValue.byteValue()));
            } else if (typeNode.equals(Identifiers.ByteString)) {
                return new Variant(ByteString.of(Base64.getDecoder().decode((String) value)));
            } else if (typeNode.equals(Identifiers.DateTime)) {
                if (value instanceof String) {
                    DateFormat df = new SimpleDateFormat(DATE_TIME_FORMAT);
                    return new Variant(new DateTime(df.parse((String) value)));
                } else {
                    return new Variant(new DateTime(new Date(numberValue.longValue())));
                }
            } else {
                Integer opcType = ((Integer) typeNode.getIdentifier());
                log.error("Failed to convert value '{}' to OPC format: unsupported OPC type {}", value, opcType);
                throw new IllegalArgumentException("Unsupported OPC type " + opcType);
            }
        } catch (ParseException e) {
            log.error("Failed to convert value '{}' to OPC format: wrong date/time format", value);
            throw new IllegalArgumentException(String.format("Wrong date/time format. Expected '%s'", DATE_TIME_FORMAT));
        } catch (Exception e) {
            log.error("Failed to cast value to opc type", e);
            throw e;
        }
    }
}
