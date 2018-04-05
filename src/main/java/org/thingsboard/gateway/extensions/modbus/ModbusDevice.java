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

package org.thingsboard.gateway.extensions.modbus;

import com.ghgande.j2mod.modbus.ModbusException;
import com.ghgande.j2mod.modbus.procimg.InputRegister;
import com.ghgande.j2mod.modbus.util.BitVector;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.gateway.extensions.modbus.conf.ModbusExtensionConstants;
import org.thingsboard.gateway.extensions.modbus.conf.mapping.DeviceMapping;
import org.thingsboard.gateway.extensions.modbus.conf.mapping.TagMapping;
import org.thingsboard.server.common.data.kv.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.LongBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
public class ModbusDevice {
    private DeviceMapping configuration;

    private Map<Integer, List<TagMapping>> tagsByPollPeriod = new HashMap<>();

    private Map<String, KvEntry> attributes = new HashMap<>();
    private Map<String, TsKvEntry> timeseries = new HashMap<>();

    private List<KvEntry> attributesUpdates = new LinkedList<>();
    private List<TsKvEntry> timeseriesUpdates = new LinkedList<>();

    public ModbusDevice(DeviceMapping conf) {
        this.configuration = conf;

        sortByPollPeriod(configuration.getAttributes(), configuration.getAttributesPollPeriod());
        sortByPollPeriod(configuration.getTimeseries(), configuration.getTimeseriesPollPeriod());

        configuration.getAttributes().stream().forEach(attr -> attributes.put(attr.getTag(), null));
    }

    private void sortByPollPeriod(List<TagMapping> mappings, int defaultPollPeriod) {
        mappings.stream().forEach(m -> {
            int pollPeriod = m.getPollPeriod();
            if (pollPeriod == ModbusExtensionConstants.NO_POLL_PERIOD_DEFINED) {
                pollPeriod = defaultPollPeriod;
            }

            tagsByPollPeriod.computeIfAbsent(pollPeriod, (k) -> new LinkedList<>()).add(m);
        });
    }

    public int getUnitId() {
        return configuration.getUnitId();
    }

    public String getName() {
        return configuration.getDeviceName();
    }

    public Map<Integer, List<TagMapping>> getSortedTagMappings() {
        return tagsByPollPeriod;
    }

    public void updateTag(TagMapping mapping, BitVector data) {
        updateTag(mapping, convertToDataEntry(mapping, data.getBit(ModbusExtensionConstants.DEFAULT_BIT_INDEX_FOR_BOOLEAN)));
    }

    public void updateTag(TagMapping mapping, InputRegister[] data) {
        updateTag(mapping, convertToDataEntry(mapping, data));
    }

    private void updateTag(TagMapping mapping, KvEntry entry) {
        if (attributes.containsKey(mapping.getTag())) {
            KvEntry oldEntry = attributes.get(mapping.getTag());
            if (oldEntry == null || !oldEntry.getValue().equals(entry.getValue())) {
                attributes.put(mapping.getTag(), entry);
                attributesUpdates.add(entry);

                log.debug("MBD[{}] attribute update: key '{}', val '{}'", configuration.getDeviceName(), entry.getKey(), entry.getValue());
            }
        } else {
            TsKvEntry oldEntry = timeseries.get(mapping.getTag());
            if (oldEntry == null || !oldEntry.getValue().equals(entry.getValue())) {
                TsKvEntry newTsEntry = new BasicTsKvEntry(System.currentTimeMillis(), entry);
                timeseries.put(mapping.getTag(), newTsEntry);
                timeseriesUpdates.add(newTsEntry);

                log.debug("MBD[{}] timeseries update:  key '{}', val '{}'", configuration.getDeviceName(), entry.getKey(), entry.getValue());
            }
        }
    }

    private KvEntry convertToDataEntry(TagMapping mapping, boolean value) {
        KvEntry entry = null;

        switch (mapping.getType().getDataType()) {
            case STRING:
                entry = new StringDataEntry(mapping.getTag(), Boolean.toString(value));
                break;
            case BOOLEAN:
                entry = new BooleanDataEntry(mapping.getTag(), value);
                break;
            case DOUBLE:
                entry = new DoubleDataEntry(mapping.getTag(), value ? 1. : 0.);
                break;
            case LONG:
                entry = new LongDataEntry(mapping.getTag(), (long)(value ? 1 : 0));
                break;
            default:
                log.error("MBD[{}] data type {} is not supported, tag [{}]", configuration.getDeviceName(), mapping.getType().getDataType(), mapping.getTag());
                throw new IllegalArgumentException("Unsupported data type " + mapping.getType().getDataType());
        }

        return entry;
    }

    private byte[] convertToLsbOrder(InputRegister[] data, String format) {
        byte[] outputBuf = new byte[data.length * 2];

        if (format.equalsIgnoreCase(ModbusExtensionConstants.LITTLE_ENDIAN_BYTE_ORDER)) {
            for (int i = 0; i < outputBuf.length; ++i) {
                outputBuf[i] = data[i / 2].toBytes()[i % 2];
            }
            return outputBuf;
        } else if (format.equalsIgnoreCase(ModbusExtensionConstants.BIG_ENDIAN_BYTE_ORDER)) {
            for (int i = outputBuf.length - 1; i >= 0; --i) {
                outputBuf[outputBuf.length - i - 1] = data[i / 2].toBytes()[i % 2];
            }
            return outputBuf;
        }

        int length = format.length();
        int foundMarkers = 0;

        for (int i = 0; i < length; ++i) {
            char c = format.charAt(i);
            int distance = -1;

            if (Character.isDigit(c)) {
                distance = c - '0';
            } else if (Character.isLetter(c)) {
                distance = Character.toLowerCase(c) - 'a';
            }

            if (distance >= 0 && outputBuf.length > distance) {
                outputBuf[distance] = data[foundMarkers / 2].toBytes()[foundMarkers % 2];
                foundMarkers++;
            }
        }

        if (foundMarkers != outputBuf.length) {
            return null;
        }

        return outputBuf;
    }

    private KvEntry convertToDataEntry(TagMapping mapping, InputRegister[] data) {
        KvEntry entry = null;

        ByteBuffer byteBuffer = ByteBuffer.wrap(convertToLsbOrder(data, mapping.getByteOrder()));
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        switch (mapping.getType().getDataType()) {
            case BOOLEAN:
                if (ModbusExtensionConstants.MIN_BIT_INDEX_IN_REG > mapping.getBit() || mapping.getBit() > ModbusExtensionConstants.MAX_BIT_INDEX_IN_REG) {
                    log.error("MBD[{}] wrong bit index {}, tag [{}]", configuration.getDeviceName(), mapping.getBit(), mapping.getTag());
                    throw new IllegalArgumentException("Bit index is out of range, value " + mapping.getBit());
                }

                entry = new BooleanDataEntry(mapping.getTag(), ((byteBuffer.getShort() >> mapping.getBit()) & 1) == 1);
                break;
            case STRING:
                entry = new StringDataEntry(mapping.getTag(), new String(byteBuffer.array()));
                break;
            case DOUBLE:
                double doubleNumber = mapping.getRegisterCount() <= 2 ? byteBuffer.getFloat() : byteBuffer.getDouble();
                entry = new DoubleDataEntry(mapping.getTag(), doubleNumber);
                break;
            case LONG:
                long longNumber = 0;
                switch (mapping.getRegisterCount()) {
                    case 1:
                        longNumber = byteBuffer.getShort();
                        break;
                    case 2:
                        longNumber = byteBuffer.getInt();
                        break;
                    case 4:
                        longNumber = byteBuffer.getLong();
                        break;
                }
                entry = new LongDataEntry(mapping.getTag(), longNumber);
                break;
            default:
                log.error("MBD[{}] data type {} is not supported, tag [{}]", configuration.getDeviceName(), mapping.getType().getDataType(), mapping.getTag());
                throw new IllegalArgumentException("Unsupported data type " + mapping.getType().getDataType());
        }

        return entry;
    }

    public List<TagMapping> getAttributesMappings() {
        return configuration.getAttributes();
    }

    public List<TagMapping> getTimeseriesMappings() {
        return configuration.getTimeseries();
    }

    public List<KvEntry> getAttributesUpdate() {
        return attributesUpdates;
    }

    public List<TsKvEntry> getTimeseriesUpdate() {
        return timeseriesUpdates;
    }

    public void clearUpdates() {
        attributesUpdates.clear();
        timeseriesUpdates.clear();
    }
}
