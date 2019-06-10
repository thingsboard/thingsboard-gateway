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
package org.thingsboard.gateway.extensions.modbus.util;

import com.ghgande.j2mod.modbus.procimg.InputRegister;
import com.ghgande.j2mod.modbus.procimg.Register;
import com.ghgande.j2mod.modbus.procimg.SimpleRegister;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.jni.Poll;
import org.thingsboard.gateway.extensions.modbus.conf.ModbusExtensionConstants;
import org.thingsboard.gateway.extensions.modbus.conf.mapping.PollingTagMapping;
import org.thingsboard.gateway.extensions.modbus.conf.mapping.TagMapping;
import org.thingsboard.server.common.data.kv.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Slf4j
public class ModbusUtils {

    public static KvEntry convertToDataEntry(PollingTagMapping mapping, boolean value) {
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
                log.error("Data type {} is not supported, tag [{}]", mapping.getType().getDataType(), mapping.getTag());
                throw new IllegalArgumentException("Unsupported data type " + mapping.getType().getDataType());
        }

        return entry;
    }

    public static byte[] toBytes(Object value, int regCount) {
        if (value instanceof Integer) {
            return toBytes((Integer) value, regCount);
        } else if (value instanceof Long) {
            return toBytes((Long) value, regCount);
        } else if (value instanceof Float) {
            return toBytes((Float) value, regCount);
        } else if (value instanceof Double) {
            return toBytes((Double) value, regCount);
        } else if (value instanceof Boolean) {
            return toBytes((Boolean) value, regCount);
        } else if (value instanceof String) {
            return toBytes((String) value, regCount);
        } else {
            throw new IllegalArgumentException("Unexpected object type " + value.getClass());
        }
    }

    public static byte[] toBytes(String value, int regCount) {
        int maxSize = regCount * 2;
        ByteBuffer buffer = ByteBuffer.allocate(maxSize)
                                        .put(value.getBytes(), 0, Math.min(value.length(), maxSize));
        int emptyBytesCount = maxSize - value.length();
        for (int i = 0; i < emptyBytesCount; ++i) {
            buffer.put((byte) 0);
        }
        return buffer.array();
    }

    public static byte[] toBytes(Float value, int regCount) {
        return toBytes(Double.valueOf(value.doubleValue()), regCount);
    }

    public static byte[] toBytes(Double value, int regCount) {
        ByteBuffer buffer = ByteBuffer.allocate(regCount * 2).order(ByteOrder.LITTLE_ENDIAN);
        if (regCount == ModbusExtensionConstants.FLOAT_REGISTER_COUNT) {
            buffer.putFloat(value.floatValue());
        } else if (regCount == ModbusExtensionConstants.DOUBLE_REGISTER_COUNT) {
            buffer.putDouble(value.doubleValue());
        } else {
            throw new IllegalArgumentException("Unexpected register count " + regCount);
        }
        return buffer.array();
    }

    public static byte[] toBytes(Integer value, int regCount) {
        return toBytes(Long.valueOf(value.longValue()), regCount);
    }

    public static byte[] toBytes(Long value, int regCount) {
        ByteBuffer buffer = ByteBuffer.allocate(regCount * 2).order(ByteOrder.LITTLE_ENDIAN);
        if (regCount == ModbusExtensionConstants.WORD_REGISTER_COUNT) {
            buffer.putShort(value.shortValue());
        } else if (regCount == ModbusExtensionConstants.INTEGER_REGISTER_COUNT) {
            buffer.putInt(value.intValue());
        } else if (regCount == ModbusExtensionConstants.LONG_REGISTER_COUNT) {
            buffer.putLong(value.longValue());
        } else {
            throw new IllegalArgumentException("Unexpected register count " + regCount);
        }
        return buffer.array();
    }

    public static byte[] formatLsbBytes(byte[] lsbData, String format) {
        if (format.equalsIgnoreCase(ModbusExtensionConstants.LITTLE_ENDIAN_BYTE_ORDER)) {
            return lsbData.clone();
        }

        byte[] formattedData = new byte[lsbData.length];
        if (format.equalsIgnoreCase(ModbusExtensionConstants.BIG_ENDIAN_BYTE_ORDER)) {
            for (int i = 0; i < formattedData.length; ++i) {
                formattedData[i] = lsbData[formattedData.length - i - 1];
            }
        } else {
            int foundMarkers = 0;

            for (int i = 0; i < format.length(); ++i) {
                char c = format.charAt(i);
                int distance = -1;

                if (Character.isDigit(c)) {
                    distance = c - '0';
                } else if (Character.isLetter(c)) {
                    distance = Character.toLowerCase(c) - 'a';
                }

                if (distance >= 0 && lsbData.length > distance) {
                    formattedData[foundMarkers] = lsbData[distance];
                    foundMarkers++;
                }
            }

            if (foundMarkers != formattedData.length) {
                throw new IllegalArgumentException(String.format("Wrong byte format '%s'", format));
            }
        }

        return formattedData;
    }

    public static Register[] toRegisters(byte[] data) {
        SimpleRegister[] registers = new SimpleRegister[data.length / 2];
        for (int i = 0; i < registers.length; ++i) {
            registers[i] = new SimpleRegister(data[i * 2], data[i * 2 + 1]);
        }
        return registers;
    }

    public static byte[] convertToLsbOrder(InputRegister[] data, String format) {
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
        }else if (format.equalsIgnoreCase(ModbusExtensionConstants.BIG_ENDIAN_BYTE_SWAP)) {
            Integer ing =data[0].getValue()+data[1].getValue()*65536;
            return toBytes(ing,data.length);
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

    public static KvEntry convertToDataEntry(PollingTagMapping mapping, InputRegister[] data) {
        KvEntry entry = null;

        ByteBuffer byteBuffer = ByteBuffer.wrap(convertToLsbOrder(data, mapping.getByteOrder()));
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        switch (mapping.getType().getDataType()) {
            case BOOLEAN:
                if (ModbusExtensionConstants.MIN_BIT_INDEX_IN_REG > mapping.getBit() || mapping.getBit() > ModbusExtensionConstants.MAX_BIT_INDEX_IN_REG) {
                    log.error("Wrong bit index {}, tag [{}]", mapping.getBit(), mapping.getTag());
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
                log.error("Data type {} is not supported, tag [{}]", mapping.getType().getDataType(), mapping.getTag());
                throw new IllegalArgumentException("Unsupported data type " + mapping.getType().getDataType());
        }

        return entry;
    }
}
